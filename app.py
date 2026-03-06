import asyncio
import json
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, asdict, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List


# =========================
# DataBus (OPC UA/Modbus stub)
# =========================
class DataBus:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tags: Dict[str, Any] = {}

    def read_all(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._tags)

    def read(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._tags.get(key, default)

    def write_many(self, values: Dict[str, Any]) -> None:
        with self._lock:
            self._tags.update(values)


@dataclass
class Operator:
    name: str
    role: str
    skill_level: float
    fatigue: float = 0.0
    busy_until_min: float = 0.0

    def update_fatigue(self, tick_min: float, working: bool) -> None:
        if working:
            self.fatigue = min(1.0, self.fatigue + 0.002)
        else:
            self.fatigue = max(0.0, self.fatigue - 0.001)

    def speed_factor(self) -> float:
        return max(0.5, self.skill_level - self.fatigue * 0.6)

    def human_error_risk(self) -> float:
        return min(0.45, (1.0 - self.skill_level) * 0.3 + self.fatigue * 0.25)


@dataclass
class PhysicsEngine:
    batch_size_kg: float = 500.0
    target_temp_c: float = 110.0
    mill_rotor_mps: float = 95.0
    color_now: str = "RAL9016"
    color_next: str = "RAL9016"

    # buffers/process
    dosing_done_kg: float = 0.0
    premix_kg: float = 0.0
    premix_elapsed_min: int = 0
    premix_target_min: int = 12
    extruder_melt_kg: float = 0.0
    chips_buffer_kg: float = 0.0
    chips_buffer_cap: float = 260.0
    mill_powder_kg: float = 0.0
    qc_hold_queue: List[Dict[str, float]] = field(default_factory=list)
    qc_hold_kg: float = 0.0
    qc_release_kg: float = 0.0
    powder_buffer_kg: float = 0.0
    powder_buffer_cap: float = 300.0
    packed_kg: float = 0.0

    # losses/recycle
    sieve_reject_kg: float = 0.0
    dust_loss_kg: float = 0.0
    classifier_recycle_kg: float = 0.0
    remill_kg: float = 0.0
    rejected_qc_kg: float = 0.0

    # sensors/6D/7D/8D
    zones_c: List[float] = field(default_factory=lambda: [95.0, 100.0, 105.0])
    dust_dp_bar: float = 0.6
    dust_lel_pct: float = 7.0
    voc_level_mg_m3: float = 12.0
    carbon_kg_co2: float = 0.0
    e_total_kwh: float = 0.0
    mill_runtime_h: float = 0.0
    liner_wear_mm: float = 0.0
    liner_wear_limit_mm: float = 8.0
    clean_cycle_h: float = 6.0

    # quality
    d50_um: float = 38.0
    d90_um: float = 86.0
    delta_e: float = 0.9

    # status
    tick_min: int = 0
    running: bool = False
    paused: bool = False
    completed: bool = False
    cip_remaining_min: int = 0
    statuses: Dict[str, str] = field(default_factory=lambda: {
        "Дозирование": "Idle",
        "Премиксер": "Idle",
        "Экструдер": "Idle",
        "Охлаждение+Дробилка": "Idle",
        "Мельница ACM": "Idle",
        "Вибросито": "Idle",
        "Фасовка": "Idle",
    })

    def reset(self, batch: float, temp: float, rotor: float, color_now: str, color_next: str) -> None:
        self.__dict__.update(PhysicsEngine().__dict__)
        self.batch_size_kg = batch * random.uniform(0.99, 1.01)
        self.target_temp_c = temp
        self.mill_rotor_mps = rotor
        self.color_now = color_now
        self.color_next = color_next
        self.premix_target_min = random.randint(10, 15)
        self.cip_remaining_min = 30 if color_now != color_next else 0
        self.running = True

    def _lerp(self, c: float, t: float, k: float) -> float:
        return c + (t - c) * k

    def step(self, dt_min: float, plc_cmd: Dict[str, Any], op_speed: Dict[str, float]) -> None:
        if not self.running or self.paused or self.completed:
            return

        self.tick_min += int(dt_min)
        for k in self.statuses:
            self.statuses[k] = "Idle"

        if self.cip_remaining_min > 0:
            self.cip_remaining_min -= 1
            return

        # Dosing
        dose_rate = (self.batch_size_kg / 5.0) * op_speed.get("Оператор-1", 1.0)
        if self.dosing_done_kg < self.batch_size_kg:
            self.statuses["Дозирование"] = "Running"
            moved = min(dose_rate, self.batch_size_kg - self.dosing_done_kg)
            self.dosing_done_kg += moved
            self.premix_kg += moved

        # Premixer
        if self.dosing_done_kg >= self.batch_size_kg and self.premix_kg > 0 and self.premix_elapsed_min < self.premix_target_min:
            self.statuses["Премиксер"] = "Running"
            self.premix_elapsed_min += 1

        premix_done = self.premix_elapsed_min >= self.premix_target_min

        # Extruder
        if premix_done and plc_cmd.get("extruder_enable", False):
            chips_fill = self.chips_buffer_kg / self.chips_buffer_cap
            derate = 0.0 if chips_fill > 0.9 else (0.5 if chips_fill > 0.75 else 1.0)
            if derate > 0:
                self.statuses["Экструдер"] = "Running"
                rate = 250.0 / 60.0 * derate
                moved = min(rate, self.premix_kg)
                self.premix_kg -= moved
                self.extruder_melt_kg += moved
            for i in range(3):
                self.zones_c[i] = self._lerp(self.zones_c[i], self.target_temp_c + (i - 1) * 3, 0.2)

        # Cooling + chips buffer
        if self.extruder_melt_kg > 0:
            self.statuses["Охлаждение+Дробилка"] = "Running"
            moved = min(320.0 / 60.0, self.extruder_melt_kg, self.chips_buffer_cap - self.chips_buffer_kg)
            self.extruder_melt_kg -= moved
            self.chips_buffer_kg += moved

        # Return from QC remill
        if self.remill_kg > 0:
            back = min(4.0, self.remill_kg)
            self.remill_kg -= back
            self.chips_buffer_kg = min(self.chips_buffer_cap, self.chips_buffer_kg + back)

        # ACM mill + classifier recycle
        if self.chips_buffer_kg > 0 and plc_cmd.get("mill_enable", False):
            self.statuses["Мельница ACM"] = "Running"
            milled = min((200 + self.mill_rotor_mps) / 60.0, self.chips_buffer_kg)
            self.chips_buffer_kg -= milled

            dust_pct = random.uniform(0.01, 0.02)
            dust = milled * dust_pct
            self.dust_loss_kg += dust
            fine_candidate = milled - dust

            # wear increases recycle fraction
            wear_factor = min(1.0, self.liner_wear_mm / self.liner_wear_limit_mm)
            recycle_frac = min(0.45, max(0.05, 0.16 + wear_factor * 0.2))
            recycle = fine_candidate * recycle_frac
            fine = fine_candidate - recycle

            self.classifier_recycle_kg += recycle
            self.chips_buffer_kg = min(self.chips_buffer_cap, self.chips_buffer_kg + recycle)
            self.mill_powder_kg += fine
            self.mill_runtime_h += dt_min / 60.0
            self.liner_wear_mm += 0.0012  # stub wear growth

        # Sieve + QC hold
        if self.mill_powder_kg > 0:
            self.statuses["Вибросито"] = "Running"
            moved = min(240.0 / 60.0, self.mill_powder_kg)
            self.mill_powder_kg -= moved
            rej = moved * random.uniform(0.005, 0.01)
            self.sieve_reject_kg += rej
            to_qc = moved - rej
            delay = random.randint(30, 45)
            self.qc_hold_queue.append({"mass": to_qc, "ready": self.tick_min + delay})

        # QC release/hold/reject
        hold = 0.0
        remain = []
        for lot in self.qc_hold_queue:
            if self.tick_min >= lot["ready"]:
                self.d50_um = max(30.0, min(50.0, 42 - (self.mill_rotor_mps - 70) * 0.2 + random.uniform(-2, 2)))
                self.d90_um = max(70.0, min(120.0, 95 - (self.mill_rotor_mps - 70) * 0.35 + random.uniform(-5, 5)))
                self.delta_e = max(0.4, min(2.5, (0.8 if self.color_now == self.color_next else 1.2) + random.uniform(-0.4, 0.6)))
                if self.d90_um <= 90 and self.delta_e <= 1.5:
                    self.qc_release_kg += lot["mass"]
                elif self.d90_um > 90:
                    self.remill_kg += lot["mass"]
                else:
                    self.rejected_qc_kg += lot["mass"]
            else:
                hold += lot["mass"]
                remain.append(lot)
        self.qc_hold_queue = remain
        self.qc_hold_kg = hold

        # move released to packing hopper
        moved_rel = min(8.0, self.qc_release_kg, self.powder_buffer_cap - self.powder_buffer_kg)
        self.qc_release_kg -= moved_rel
        self.powder_buffer_kg += moved_rel

        # Packing manual speed operator-1
        if self.powder_buffer_kg >= 20 and plc_cmd.get("packing_enable", True):
            self.statuses["Фасовка"] = "Running"
            boxes = 1 if op_speed.get("Оператор-1", 1.0) < 0.9 else 2
            pack = min(self.powder_buffer_kg, boxes * 20)
            pack -= pack % 20
            self.powder_buffer_kg -= pack
            self.packed_kg += pack

        # Sensor dynamics / 6D/8D
        mill_run = self.statuses["Мельница ACM"] == "Running"
        self.dust_dp_bar = max(0.4, min(2.5, self.dust_dp_bar + (0.02 if mill_run else -0.01)))
        self.dust_lel_pct = max(0.0, min(40.0, self.dust_lel_pct + (1.3 if mill_run else -0.7) + (0.4 if not plc_cmd.get("vent_on", True) else -0.3)))

        # energy + ecology
        kw = 0.0
        nominal = {"Дозирование": 4, "Премиксер": 22, "Экструдер": 55, "Охлаждение+Дробилка": 9, "Мельница ACM": 11, "Вибросито": 3, "Фасовка": 4}
        for n, st in self.statuses.items():
            kw += nominal[n] if st == "Running" else nominal[n] * 0.2
        self.e_total_kwh += kw / 60.0
        self.voc_level_mg_m3 = max(2.0, min(120.0, self.voc_level_mg_m3 + (0.6 if self.statuses["Экструдер"] == "Running" else -0.2)))
        self.carbon_kg_co2 = self.e_total_kwh * 0.42

        in_proc = self.premix_kg + self.extruder_melt_kg + self.chips_buffer_kg + self.mill_powder_kg + self.qc_hold_kg + self.powder_buffer_kg + self.qc_release_kg
        if self.dosing_done_kg >= self.batch_size_kg and self.premix_elapsed_min >= self.premix_target_min and in_proc < 0.001:
            self.completed = True
            self.running = False


class PLCController:
    def __init__(self) -> None:
        self.alarm_latch: Dict[str, bool] = {}
        self.trip = False
        self.warning = False

    def step(self, tags: Dict[str, Any]) -> Dict[str, Any]:
        vent_on = bool(tags.get("Vent_on", True))
        atex_ok = bool(tags.get("ATEX_ok", True))
        ground_ok = bool(tags.get("Ground_ok", True))
        dust_lel = float(tags.get("Dust_LEL_pct", 0.0))
        dust_dp = float(tags.get("Dust_DP_bar", 0.0))

        permissive = vent_on and atex_ok and ground_ok
        self.warning = dust_lel > 15.0 or dust_dp > 1.8
        self.trip = dust_lel > 25.0

        self.alarm_latch["LEL_WARN"] = dust_lel > 15.0
        self.alarm_latch["FILTER_DP_WARN"] = dust_dp > 1.8
        self.alarm_latch["DUST_TRIP"] = self.trip
        self.alarm_latch["PERMISSIVE_BLOCK"] = not permissive

        return {
            "extruder_enable": permissive and not self.trip,
            "mill_enable": permissive and not self.trip,
            "packing_enable": not self.trip,
            "vent_on": vent_on,
            "trip": self.trip,
            "warning": self.warning,
            "alarms": dict(self.alarm_latch),
        }


class TwinRuntime:
    def __init__(self) -> None:
        self.bus = DataBus()
        self.physics = PhysicsEngine()
        self.plc = PLCController()
        self.lock = threading.Lock()
        self.commands: deque = deque()

        self.operators = [
            Operator("Оператор-1", "operator", 0.82),
            Operator("Оператор-2", "operator", 0.88),
            Operator("Лаборант QC", "lab_QC", 0.91),
            Operator("Инженер СГИ", "mechanic", 0.93),
        ]

        self.bus.write_many({"ATEX_ok": True, "Ground_ok": True, "Vent_on": True})

    def push_command(self, cmd: Dict[str, Any]) -> None:
        with self.lock:
            self.commands.append(cmd)

    def _handle_commands(self) -> None:
        with self.lock:
            while self.commands:
                cmd = self.commands.popleft()
                action = cmd.get("action")
                if action == "start":
                    self.physics.reset(
                        float(cmd.get("batch_size_kg", 500)),
                        float(cmd.get("target_temp_c", 110)),
                        float(cmd.get("mill_rotor_mps", 95)),
                        str(cmd.get("color_now", "RAL9016")),
                        str(cmd.get("color_next", "RAL9016")),
                    )
                elif action == "pause":
                    self.physics.paused = True
                elif action == "resume":
                    self.physics.paused = False
                    self.physics.running = True
                elif action == "inject_fault":
                    code = random.choice(["EXTR_ZONE_HOT", "FILTER_DP_HIGH", "PACKER_JAM", "NO_PPE"])
                    if code == "EXTR_ZONE_HOT":
                        self.physics.zones_c[1] = 126
                    if code == "FILTER_DP_HIGH":
                        self.physics.dust_dp_bar = 2.1
                    if code == "NO_PPE":
                        self.plc.alarm_latch["NO_PPE"] = True
                    self.physics.paused = True
                    self.physics.running = False
                elif action == "clear_fault":
                    self.physics.running = not self.physics.completed
                    self.physics.paused = False
                    self.plc.alarm_latch["NO_PPE"] = False
                elif action == "set_permissives":
                    self.bus.write_many({
                        "ATEX_ok": bool(cmd.get("ATEX_ok", True)),
                        "Ground_ok": bool(cmd.get("Ground_ok", True)),
                        "Vent_on": bool(cmd.get("Vent_on", True)),
                    })

    def snapshot(self) -> Dict[str, Any]:
        p = self.physics
        losses = p.dust_loss_kg + p.sieve_reject_kg + p.rejected_qc_kg
        return {
            "tick_min": p.tick_min,
            "running": p.running,
            "paused": p.paused,
            "completed": p.completed,
            "statuses": p.statuses,
            "masses": {
                "batch": p.batch_size_kg,
                "premix": p.premix_kg,
                "melt": p.extruder_melt_kg,
                "chips_buffer": p.chips_buffer_kg,
                "powder_buffer": p.powder_buffer_kg,
                "packed": p.packed_kg,
            },
            "quality": {"D50_um": p.d50_um, "D90_um": p.d90_um, "DeltaE": p.delta_e},
            "kpi": {
                "Yield_pct": (p.packed_kg / p.batch_size_kg * 100) if p.batch_size_kg else 0,
                "Losses_pct": (losses / p.batch_size_kg * 100) if p.batch_size_kg else 0,
                "E_total_kWh": p.e_total_kwh,
                "E_spec_kWh_kg": (p.e_total_kwh / p.packed_kg) if p.packed_kg else 0,
                "VOC_mg_m3": p.voc_level_mg_m3,
                "Carbon_kgCO2": p.carbon_kg_co2,
            },
            "sensors": {
                "Zones_C": p.zones_c,
                "Dust_DP_bar": p.dust_dp_bar,
                "Dust_LEL_pct": p.dust_lel_pct,
                "Liner_wear_mm": p.liner_wear_mm,
                "Mill_runtime_h": p.mill_runtime_h,
                "QC_hold_kg": p.qc_hold_kg,
                "CIP_remaining_min": p.cip_remaining_min,
            },
            "plc": {
                "trip": self.plc.trip,
                "warning": self.plc.warning,
                "alarms": self.plc.alarm_latch,
            },
            "personnel": [asdict(o) | {"speed_factor": o.speed_factor(), "error_risk": o.human_error_risk()} for o in self.operators],
        }

    async def run(self) -> None:
        while True:
            self._handle_commands()

            tags = self.bus.read_all()
            tags.update({
                "Dust_LEL_pct": self.physics.dust_lel_pct,
                "Dust_DP_bar": self.physics.dust_dp_bar,
            })
            plc_cmd = self.plc.step(tags)

            op_speed = {}
            for op in self.operators:
                busy = self.physics.running and not self.physics.paused and not self.physics.completed
                op.update_fatigue(self.physics.tick_min, busy)
                op_speed[op.name] = op.speed_factor()

            self.physics.step(1, plc_cmd, op_speed)
            await asyncio.sleep(0.5)


RUNTIME = TwinRuntime()


class Handler(BaseHTTPRequestHandler):
    def _json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.end_headers()

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            content = Path("index.html").read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return
        if self.path == "/api/state":
            self._json(RUNTIME.snapshot())
            return
        self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        if self.path != "/api/command":
            self._json({"error": "not found"}, 404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b"{}"
        data = json.loads(body.decode("utf-8"))
        RUNTIME.push_command(data)
        self._json({"ok": True})


def main() -> None:
    loop = asyncio.new_event_loop()

    def _run_loop() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(RUNTIME.run())

    t = threading.Thread(target=_run_loop, daemon=True)
    t.start()

    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    print("Digital Twin server: http://0.0.0.0:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
