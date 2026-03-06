import asyncio
import json
import random
import threading
from dataclasses import dataclass, asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List


class DataBus:
    """Простая шина тегов (заглушка OPC UA / Modbus)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tags: Dict[str, Any] = {
            "ATEX_ok": True,
            "Ground_ok": True,
            "Vent_on": True,
        }

    def read_all(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._tags)

    def write_many(self, tags: Dict[str, Any]) -> None:
        with self._lock:
            self._tags.update(tags)


@dataclass
class Operator:
    name: str
    role: str
    skill_level: float
    fatigue: float = 0.0

    def step(self, active: bool) -> None:
        if active:
            self.fatigue = min(1.0, self.fatigue + 0.002)
        else:
            self.fatigue = max(0.0, self.fatigue - 0.001)

    @property
    def speed_factor(self) -> float:
        return max(0.5, self.skill_level - 0.6 * self.fatigue)

    @property
    def error_risk(self) -> float:
        return min(0.45, (1 - self.skill_level) * 0.30 + self.fatigue * 0.25)


class PhysicsEngine:
    """Только физика/динамика процесса и сенсоры."""

    NODE_NAMES = [
        "Дозирование",
        "Премиксер",
        "Экструдер",
        "Охлаждение+Дробилка",
        "Мельница ACM",
        "Вибросито",
        "Фасовка",
    ]

    def __init__(self) -> None:
        self.reset(500, 110, 95, "RAL9016", "RAL9016")
        self.running = False

    def reset(self, batch_kg: float, target_temp_c: float, mill_rotor_mps: float, color_now: str, color_next: str) -> None:
        self.batch_size_kg = batch_kg * random.uniform(0.99, 1.01)
        self.target_temp_c = float(target_temp_c)
        self.mill_rotor_mps = float(mill_rotor_mps)
        self.color_now = color_now
        self.color_next = color_next

        self.tick_min = 0
        self.running = True
        self.paused = False
        self.completed = False

        self.cip_remaining_min = 30 if color_now != color_next else 0

        self.statuses = {n: "Idle" for n in self.NODE_NAMES}

        # Потоки
        self.dosed_kg = 0.0
        self.premix_kg = 0.0
        self.premix_elapsed_min = 0
        self.premix_target_min = random.randint(10, 15)
        self.melt_kg = 0.0
        self.chips_buffer_kg = 0.0
        self.chips_buffer_cap_kg = 260.0
        self.mill_powder_kg = 0.0
        self.qc_release_kg = 0.0
        self.qc_hold_kg = 0.0
        self.powder_buffer_kg = 0.0
        self.powder_buffer_cap_kg = 320.0
        self.packed_kg = 0.0

        # Потери/рециклы
        self.sieve_reject_kg = 0.0
        self.dust_loss_kg = 0.0
        self.classifier_recycle_kg = 0.0
        self.remill_kg = 0.0
        self.qc_reject_kg = 0.0

        self.qc_queue: List[Dict[str, float]] = []

        # Сенсоры и 6D/7D/8D
        self.zones_c = [95.0, 100.0, 105.0]
        self.dust_dp_bar = 0.6
        self.dust_lel_pct = 7.0
        self.voc_level_mg_m3 = 12.0
        self.e_total_kwh = 0.0
        self.carbon_kg_co2 = 0.0
        self.mill_runtime_h = 0.0
        self.liner_wear_mm = 0.0
        self.liner_wear_limit_mm = 8.0

        # Качество
        self.d50_um = 38.0
        self.d90_um = 86.0
        self.delta_e = 0.9

    @staticmethod
    def _lerp(cur: float, target: float, alpha: float) -> float:
        return cur + (target - cur) * alpha

    def step(self, plc_cmd: Dict[str, Any], operator_speed: float) -> None:
        if not self.running or self.paused or self.completed:
            return

        self.tick_min += 1
        self.statuses = {n: "Idle" for n in self.NODE_NAMES}

        if self.cip_remaining_min > 0:
            self.cip_remaining_min -= 1
            self._update_energy_and_env()
            return

        # 1) Дозирование
        dose_rate = (self.batch_size_kg / 5.0) * operator_speed
        if self.dosed_kg < self.batch_size_kg:
            self.statuses["Дозирование"] = "Running"
            moved = min(dose_rate, self.batch_size_kg - self.dosed_kg)
            self.dosed_kg += moved
            self.premix_kg += moved

        # 2) Премиксер
        if self.dosed_kg >= self.batch_size_kg and self.premix_kg > 0 and self.premix_elapsed_min < self.premix_target_min:
            self.statuses["Премиксер"] = "Running"
            self.premix_elapsed_min += 1
        premix_done = self.premix_elapsed_min >= self.premix_target_min

        # 3) Экструдер + bottleneck
        if premix_done and plc_cmd.get("extruder_enable", False):
            fill = self.chips_buffer_kg / self.chips_buffer_cap_kg
            derate = 0.0 if fill > 0.90 else (0.5 if fill > 0.75 else 1.0)
            if derate > 0:
                self.statuses["Экструдер"] = "Running"
                moved = min(250.0 / 60.0 * derate, self.premix_kg)
                self.premix_kg -= moved
                self.melt_kg += moved

            for i in range(3):
                self.zones_c[i] = self._lerp(self.zones_c[i], self.target_temp_c + (i - 1) * 3, 0.2)

        # 4) Охлаждение + дробилка
        if self.melt_kg > 0:
            self.statuses["Охлаждение+Дробилка"] = "Running"
            moved = min(320.0 / 60.0, self.melt_kg, self.chips_buffer_cap_kg - self.chips_buffer_kg)
            self.melt_kg -= moved
            self.chips_buffer_kg += moved

        # Remill возврат
        if self.remill_kg > 0:
            back = min(4.0, self.remill_kg)
            self.remill_kg -= back
            self.chips_buffer_kg = min(self.chips_buffer_cap_kg, self.chips_buffer_kg + back)

        # 5) ACM + классификатор
        if self.chips_buffer_kg > 0 and plc_cmd.get("mill_enable", False):
            self.statuses["Мельница ACM"] = "Running"
            moved = min((200 + self.mill_rotor_mps) / 60.0, self.chips_buffer_kg)
            self.chips_buffer_kg -= moved

            dust = moved * random.uniform(0.01, 0.02)
            self.dust_loss_kg += dust
            useful = moved - dust

            wear = min(1.0, self.liner_wear_mm / self.liner_wear_limit_mm)
            recycle_frac = min(0.45, max(0.05, 0.16 + wear * 0.2))
            recycle = useful * recycle_frac
            fine = useful - recycle

            self.classifier_recycle_kg += recycle
            self.chips_buffer_kg = min(self.chips_buffer_cap_kg, self.chips_buffer_kg + recycle)
            self.mill_powder_kg += fine

            self.mill_runtime_h += 1 / 60.0
            self.liner_wear_mm += 0.0012

        # 6) Вибросито + QC очередь
        if self.mill_powder_kg > 0:
            self.statuses["Вибросито"] = "Running"
            moved = min(240.0 / 60.0, self.mill_powder_kg)
            self.mill_powder_kg -= moved
            reject = moved * random.uniform(0.005, 0.01)
            self.sieve_reject_kg += reject
            to_qc = moved - reject
            delay = random.randint(30, 45)
            self.qc_queue.append({"mass": to_qc, "ready_tick": self.tick_min + delay})

        # QC
        hold = 0.0
        remaining = []
        for lot in self.qc_queue:
            if self.tick_min >= lot["ready_tick"]:
                self.d50_um = max(30.0, min(50.0, 42 - (self.mill_rotor_mps - 70) * 0.2 + random.uniform(-2, 2)))
                self.d90_um = max(70.0, min(120.0, 95 - (self.mill_rotor_mps - 70) * 0.35 + random.uniform(-5, 5)))
                self.delta_e = max(0.4, min(2.5, (0.8 if self.color_now == self.color_next else 1.2) + random.uniform(-0.4, 0.6)))

                if self.d90_um <= 90 and self.delta_e <= 1.5:
                    self.qc_release_kg += lot["mass"]
                elif self.d90_um > 90:
                    self.remill_kg += lot["mass"]
                else:
                    self.qc_reject_kg += lot["mass"]
            else:
                hold += lot["mass"]
                remaining.append(lot)
        self.qc_queue = remaining
        self.qc_hold_kg = hold

        # Буфер перед фасовкой
        moved = min(8.0, self.qc_release_kg, self.powder_buffer_cap_kg - self.powder_buffer_kg)
        self.qc_release_kg -= moved
        self.powder_buffer_kg += moved

        # 7) Фасовка
        if self.powder_buffer_kg >= 20 and plc_cmd.get("packing_enable", True):
            self.statuses["Фасовка"] = "Running"
            boxes = 1 if operator_speed < 0.9 else 2
            packed = min(self.powder_buffer_kg, boxes * 20)
            packed -= packed % 20
            self.powder_buffer_kg -= packed
            self.packed_kg += packed

        self._update_sensors(plc_cmd)
        self._update_energy_and_env()

        in_process = self.premix_kg + self.melt_kg + self.chips_buffer_kg + self.mill_powder_kg + self.powder_buffer_kg + self.qc_hold_kg + self.qc_release_kg
        if self.dosed_kg >= self.batch_size_kg and self.premix_elapsed_min >= self.premix_target_min and in_process < 0.001:
            self.completed = True
            self.running = False

    def _update_sensors(self, plc_cmd: Dict[str, Any]) -> None:
        mill_running = self.statuses["Мельница ACM"] == "Running"
        self.dust_dp_bar = max(0.4, min(2.5, self.dust_dp_bar + (0.02 if mill_running else -0.01)))
        self.dust_lel_pct = max(
            0.0,
            min(
                40.0,
                self.dust_lel_pct + (1.3 if mill_running else -0.7) + (0.4 if not plc_cmd.get("vent_on", True) else -0.3),
            ),
        )

    def _update_energy_and_env(self) -> None:
        nominal_kw = {
            "Дозирование": 4,
            "Премиксер": 22,
            "Экструдер": 55,
            "Охлаждение+Дробилка": 9,
            "Мельница ACM": 11,
            "Вибросито": 3,
            "Фасовка": 4,
        }
        kw = 0.0
        for node, st in self.statuses.items():
            kw += nominal_kw[node] if st == "Running" else nominal_kw[node] * 0.2
        self.e_total_kwh += kw / 60.0

        self.voc_level_mg_m3 = max(2.0, min(120.0, self.voc_level_mg_m3 + (0.6 if self.statuses["Экструдер"] == "Running" else -0.2)))
        self.carbon_kg_co2 = self.e_total_kwh * 0.42


class PLCController:
    """Только логика PLC/блокировок/алармов."""

    def __init__(self) -> None:
        self.warning = False
        self.trip = False
        self.alarms: Dict[str, bool] = {}

    def step(self, tags: Dict[str, Any]) -> Dict[str, Any]:
        vent_on = bool(tags.get("Vent_on", True))
        atex_ok = bool(tags.get("ATEX_ok", True))
        ground_ok = bool(tags.get("Ground_ok", True))
        dust_lel = float(tags.get("Dust_LEL_pct", 0.0))
        dust_dp = float(tags.get("Dust_DP_bar", 0.0))

        permissive_ok = vent_on and atex_ok and ground_ok

        self.warning = dust_lel > 15.0 or dust_dp > 1.8
        self.trip = dust_lel > 25.0

        self.alarms = {
            "LEL_WARN": dust_lel > 15.0,
            "FILTER_DP_WARN": dust_dp > 1.8,
            "DUST_TRIP": self.trip,
            "PERMISSIVE_BLOCK": not permissive_ok,
        }

        return {
            "extruder_enable": permissive_ok and not self.trip,
            "mill_enable": permissive_ok and not self.trip,
            "packing_enable": not self.trip,
            "vent_on": vent_on,
        }


class TwinRuntime:
    def __init__(self) -> None:
        self.bus = DataBus()
        self.physics = PhysicsEngine()
        self.plc = PLCController()
        self.lock = threading.Lock()
        self.commands: List[Dict[str, Any]] = []

        self.personnel = [
            Operator("Оператор-1", "operator", 0.82),
            Operator("Оператор-2", "operator", 0.88),
            Operator("Лаборант QC", "lab_QC", 0.91),
            Operator("Инженер СГИ", "mechanic", 0.93),
        ]

    def push_command(self, cmd: Dict[str, Any]) -> None:
        with self.lock:
            self.commands.append(cmd)

    def _drain_commands(self) -> None:
        with self.lock:
            queue = self.commands[:]
            self.commands.clear()

        for cmd in queue:
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
                self.physics.running = not self.physics.completed
            elif action == "inject_fault":
                fault = random.choice(["EXTR_ZONE_HOT", "FILTER_DP_HIGH", "PACKER_JAM", "NO_PPE"])
                if fault == "EXTR_ZONE_HOT":
                    self.physics.zones_c[1] = 126.0
                elif fault == "FILTER_DP_HIGH":
                    self.physics.dust_dp_bar = 2.1
                self.physics.paused = True
                self.physics.running = False
                self.plc.alarms[fault] = True
            elif action == "clear_fault":
                self.physics.paused = False
                self.physics.running = not self.physics.completed
            elif action == "set_permissives":
                self.bus.write_many(
                    {
                        "ATEX_ok": bool(cmd.get("ATEX_ok", True)),
                        "Ground_ok": bool(cmd.get("Ground_ok", True)),
                        "Vent_on": bool(cmd.get("Vent_on", True)),
                    }
                )

    def snapshot(self) -> Dict[str, Any]:
        p = self.physics
        losses_kg = p.dust_loss_kg + p.sieve_reject_kg + p.qc_reject_kg
        return {
            "tick_min": p.tick_min,
            "running": p.running,
            "paused": p.paused,
            "completed": p.completed,
            "statuses": p.statuses,
            "masses": {
                "batch": p.batch_size_kg,
                "premix": p.premix_kg,
                "melt": p.melt_kg,
                "chips_buffer": p.chips_buffer_kg,
                "powder_buffer": p.powder_buffer_kg,
                "packed": p.packed_kg,
            },
            "quality": {"D50_um": p.d50_um, "D90_um": p.d90_um, "DeltaE": p.delta_e},
            "kpi": {
                "Yield_pct": (p.packed_kg / p.batch_size_kg * 100) if p.batch_size_kg else 0,
                "Losses_pct": (losses_kg / p.batch_size_kg * 100) if p.batch_size_kg else 0,
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
                "warning": self.plc.warning,
                "trip": self.plc.trip,
                "alarms": self.plc.alarms,
            },
            "personnel": [asdict(o) | {"speed_factor": o.speed_factor, "error_risk": o.error_risk} for o in self.personnel],
        }

    async def run(self) -> None:
        while True:
            self._drain_commands()

            tags = self.bus.read_all()
            tags.update(
                {
                    "Dust_DP_bar": self.physics.dust_dp_bar,
                    "Dust_LEL_pct": self.physics.dust_lel_pct,
                }
            )
            plc_cmd = self.plc.step(tags)

            active = self.physics.running and not self.physics.paused and not self.physics.completed
            for op in self.personnel:
                op.step(active)

            operator1_speed = self.personnel[0].speed_factor
            self.physics.step(plc_cmd, operator1_speed)

            await asyncio.sleep(0.5)


RUNTIME = TwinRuntime()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
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
            self._send_json(RUNTIME.snapshot())
            return

        self._send_json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        if self.path != "/api/command":
            self._send_json({"error": "not found"}, 404)
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json({"error": "invalid json"}, 400)
            return

        RUNTIME.push_command(payload)
        self._send_json({"ok": True})


def main() -> None:
    loop = asyncio.new_event_loop()

    def runner() -> None:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(RUNTIME.run())

    threading.Thread(target=runner, daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", 8000), Handler)
    print("Digital Twin running on http://0.0.0.0:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
