import asyncio
import os
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Set

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"

ANSI_REGEX = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class RunnerManager:
    def __init__(self):
        self.process: subprocess.Popen | None = None
        self.status: str = "idle"  # "idle" | "running" | "logging_in" | "stopping"
        self.current_task: str | None = None  # "apply" | "login"
        self.current_site: str = "all"
        self.ws_clients: Set[Any] = set()
        self.logs: List[str] = []
        self.max_logs: int = 500
        self.stats: Dict[str, int] = {
            "total_checked": 0,
            "applied": 0,
            "skipped": 0,
            "failed": 0
        }
        self.current_job: Dict[str, Any] = {
            "title": "",
            "company": "",
            "location": "",
            "score": None,
            "status": "Waiting to start..."
        }
        self.login_thread: threading.Thread | None = None
        self.stop_requested: bool = False

    def add_ws(self, ws):
        self.ws_clients.add(ws)

    def remove_ws(self, ws):
        self.ws_clients.discard(ws)

    async def broadcast(self, message: Dict[str, Any]):
        dead = []
        for ws in list(self.ws_clients):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.ws_clients.discard(ws)

    def log(self, text: str):
        clean = ANSI_REGEX.sub('', text).strip()
        if not clean:
            return
        timestamp = time.strftime("%H:%M:%S")
        entry = f"[{timestamp}] {clean}"
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

        try:
            log_file = DATA_DIR / "runner.log"
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(entry + "\n")
        except Exception:
            pass

        # Broadcast via event loop if active
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.broadcast({"type": "log", "text": entry}), loop
                )
        except Exception:
            pass

    def check_cookie_session(self) -> Dict[str, bool]:
        """Check if authenticated cookies exist for naukri and linkedin in their isolated profiles."""
        res = {"naukri": False, "linkedin": False}

        def _read_cookie_db(path):
            if not path.exists():
                return b""
            try:
                if sys.platform == "win32":
                    import ctypes
                    from ctypes import wintypes
                    k32 = ctypes.windll.kernel32
                    h = k32.CreateFileW(str(path), 0x80000000, 3, None, 3, 0, None)
                    if h != -1:
                        buf = ctypes.create_string_buffer(5000000)
                        br = wintypes.DWORD()
                        k32.ReadFile(h, buf, 5000000, ctypes.byref(br), None)
                        k32.CloseHandle(h)
                        return buf.raw[:br.value]
                return path.read_bytes()
            except Exception:
                return b""

        # Check Naukri dedicated profile, fallback to legacy
        naukri_db = DATA_DIR / "chrome-profile-naukri" / "Default" / "Network" / "Cookies"
        if not naukri_db.exists():
            naukri_db = DATA_DIR / "chrome-profile" / "Default" / "Network" / "Cookies"
        naukri_content = _read_cookie_db(naukri_db)
        res["naukri"] = b"naukri" in naukri_content and len(naukri_content) > 500

        # Check LinkedIn dedicated profile
        linkedin_db = DATA_DIR / "chrome-profile-linkedin" / "Default" / "Network" / "Cookies"
        linkedin_content = _read_cookie_db(linkedin_db)
        if not linkedin_content:
            # Fallback to shared legacy
            linkedin_db = DATA_DIR / "chrome-profile" / "Default" / "Network" / "Cookies"
            linkedin_content = _read_cookie_db(linkedin_db)
        res["linkedin"] = b"li_at" in linkedin_content

        return res

    def get_applications_today_count(self) -> int:
        db_path = DATA_DIR / "applications.db"
        if not db_path.exists():
            return 0
        try:
            conn = sqlite3.connect(db_path)
            today_str = time.strftime("%Y-%m-%d")
            count = conn.execute(
                "SELECT COUNT(*) FROM applications WHERE status IN ('applied', 'applied_manual') AND updated_at LIKE ?",
                (f"{today_str}%",)
            ).fetchone()[0]
            conn.close()
            return count
        except Exception:
            return 0

    def get_today_summary(self) -> Dict[str, int]:
        db_path = DATA_DIR / "applications.db"
        default_res = {
            "applied": 0, "failed": 0,
            "linkedin_applied": 0, "linkedin_failed": 0,
            "naukri_applied": 0, "naukri_failed": 0
        }
        if not db_path.exists():
            return default_res
        try:
            conn = sqlite3.connect(db_path)
            today_str = time.strftime("%Y-%m-%d")
            row = conn.execute("""
                SELECT 
                    SUM(CASE WHEN status IN ('applied', 'applied_manual') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status NOT IN ('applied', 'applied_manual') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN site = 'linkedin' AND status IN ('applied', 'applied_manual') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN site = 'linkedin' AND status NOT IN ('applied', 'applied_manual') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN site = 'naukri' AND status IN ('applied', 'applied_manual') THEN 1 ELSE 0 END),
                    SUM(CASE WHEN site = 'naukri' AND status NOT IN ('applied', 'applied_manual') THEN 1 ELSE 0 END)
                FROM applications WHERE updated_at LIKE ?
            """, (f"{today_str}%",)).fetchone()
            conn.close()
            if row:
                return {
                    "applied": row[0] or 0,
                    "failed": row[1] or 0,
                    "linkedin_applied": row[2] or 0,
                    "linkedin_failed": row[3] or 0,
                    "naukri_applied": row[4] or 0,
                    "naukri_failed": row[5] or 0
                }
            return default_res
        except Exception:
            return default_res

    def get_datewise_analytics(self) -> Dict[str, Any]:
        db_path = DATA_DIR / "applications.db"
        empty_res = {"today": self.get_today_summary(), "overall": {}, "dates": []}
        if not db_path.exists():
            return empty_res
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            today_str = time.strftime("%Y-%m-%d")

            rows = conn.execute("""
                SELECT 
                    substr(updated_at, 1, 10) as app_date,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN status IN ('applied', 'applied_manual') THEN 1 ELSE 0 END) as total_applied,
                    SUM(CASE WHEN status NOT IN ('applied', 'applied_manual') THEN 1 ELSE 0 END) as total_failed,
                    SUM(CASE WHEN site = 'linkedin' AND status IN ('applied', 'applied_manual') THEN 1 ELSE 0 END) as linkedin_applied,
                    SUM(CASE WHEN site = 'linkedin' AND status NOT IN ('applied', 'applied_manual') THEN 1 ELSE 0 END) as linkedin_failed,
                    SUM(CASE WHEN site = 'naukri' AND status IN ('applied', 'applied_manual') THEN 1 ELSE 0 END) as naukri_applied,
                    SUM(CASE WHEN site = 'naukri' AND status NOT IN ('applied', 'applied_manual') THEN 1 ELSE 0 END) as naukri_failed
                FROM applications
                WHERE updated_at IS NOT NULL AND updated_at != ''
                GROUP BY app_date
                ORDER BY app_date DESC
            """).fetchall()

            dates = []
            overall = {
                "total_applied": 0, "total_failed": 0,
                "linkedin_applied": 0, "linkedin_failed": 0,
                "naukri_applied": 0, "naukri_failed": 0,
                "total_processed": 0
            }
            today_stats = {
                "date": today_str,
                "total_applied": 0, "total_failed": 0,
                "linkedin_applied": 0, "linkedin_failed": 0,
                "naukri_applied": 0, "naukri_failed": 0,
                "success_rate": 0.0
            }

            for r in rows:
                d = dict(r)
                dt = d["app_date"]
                app = d["total_applied"] or 0
                fail = d["total_failed"] or 0
                total = app + fail
                rate = round((app / total * 100), 1) if total > 0 else 0.0
                d["success_rate"] = rate

                try:
                    parsed_dt = datetime.strptime(dt, "%Y-%m-%d")
                    if dt == today_str:
                        display = "Today (" + parsed_dt.strftime("%d %b %Y") + ")"
                    else:
                        display = parsed_dt.strftime("%d %b %Y")
                except Exception:
                    display = dt
                d["display_date"] = display
                dates.append(d)

                # Overall accumulator
                overall["total_applied"] += app
                overall["total_failed"] += fail
                overall["linkedin_applied"] += (d["linkedin_applied"] or 0)
                overall["linkedin_failed"] += (d["linkedin_failed"] or 0)
                overall["naukri_applied"] += (d["naukri_applied"] or 0)
                overall["naukri_failed"] += (d["naukri_failed"] or 0)
                overall["total_processed"] += total

                if dt == today_str:
                    today_stats = {
                        "date": today_str,
                        "total_applied": app,
                        "total_failed": fail,
                        "linkedin_applied": d["linkedin_applied"] or 0,
                        "linkedin_failed": d["linkedin_failed"] or 0,
                        "naukri_applied": d["naukri_applied"] or 0,
                        "naukri_failed": d["naukri_failed"] or 0,
                        "success_rate": rate
                    }

            tot_overall = overall["total_applied"] + overall["total_failed"]
            overall["success_rate"] = round((overall["total_applied"] / tot_overall * 100), 1) if tot_overall > 0 else 0.0

            li_tot = overall["linkedin_applied"] + overall["linkedin_failed"]
            overall["linkedin_success_rate"] = round((overall["linkedin_applied"] / li_tot * 100), 1) if li_tot > 0 else 0.0

            nk_tot = overall["naukri_applied"] + overall["naukri_failed"]
            overall["naukri_success_rate"] = round((overall["naukri_applied"] / nk_tot * 100), 1) if nk_tot > 0 else 0.0

            conn.close()
            return {"today": today_stats, "overall": overall, "dates": dates}
        except Exception as e:
            print(f"[runner_manager] get_datewise_analytics error: {e}")
            return empty_res

    def start_login(self, site: str):
        if self.status != "idle":
            raise RuntimeError("A task is already running. Please wait or stop it first.")

        self.status = "logging_in"
        self.current_task = "login"
        self.current_site = site
        self.log(f"Starting browser login for {site.upper()}...")
        self.log("Please log in to your account in the browser window that appears.")

        def run_login():
            proc = None
            try:
                # We invoke login script that monitors page URL
                cmd = [
                    sys.executable, "-u", "-m", "src.main", "login", "--site", site
                ]
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(ROOT_DIR),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    encoding="utf-8",
                    errors="replace"
                )
                self.process = proc

                for line in iter(proc.stdout.readline, ''):
                    if not line:
                        break
                    clean_line = ANSI_REGEX.sub('', line).strip()
                    self.log(clean_line)
                    if "Logged in to" in clean_line:
                        self.log(f"Success! {site.upper()} is logged in.")

                if proc:
                    proc.wait()
            except Exception as e:
                self.log(f"Login error: {e}")
            finally:
                self.status = "idle"
                self.current_task = None
                self.process = None
                auth = self.check_cookie_session()
                if auth.get(site):
                    self.log(f"✅ Verified: {site.upper()} session authenticated successfully.")
                else:
                    self.log(f"⚠️ Note: {site.upper()} was not verified as logged in. Click Re-authenticate anytime to log in.")
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            self.broadcast({"type": "status", "status": "idle"}), loop
                        )
                except Exception:
                    pass

        self.login_thread = threading.Thread(target=run_login, daemon=True)
        self.login_thread.start()

    def confirm_login_input(self):
        """Simulate pressing Enter in the login CLI prompt."""
        if self.process and self.process.stdin and self.status == "logging_in":
            try:
                self.process.stdin.write("\n")
                self.process.stdin.flush()
                self.log("Confirmation sent to browser.")
            except Exception as e:
                self.log(f"Error sending confirm: {e}")

    def start_apply(self, site: str = "naukri", limit: int = 25, auto: bool = True,
                    retry: bool = True, dry_run: bool = False, profile: str | None = None):
        if self.status != "idle":
            raise RuntimeError("Application runner is already running!")

        self.status = "running"
        self.current_task = "apply"
        self.current_site = site
        self.stop_requested = False
        self.stats = {"total_checked": 0, "applied": 0, "skipped": 0, "failed": 0}
        self.current_job = {"title": "", "company": "", "location": "", "score": None, "status": "Initializing search..."}

        cmd = [sys.executable, "-u", "-m", "src.main", "run", "--site", site, "--no-ask"]
        if limit:
            cmd.extend(["--limit", str(limit)])
        if auto:
            cmd.append("--auto")
        if retry:
            cmd.append("--retry")
        if dry_run:
            cmd.append("--dry-run")
        if profile:
            cmd.extend(["--profile", profile])

        self.log(f"Starting Auto-Apply: Site={site.upper()}, Limit={limit}, Auto={auto}, DryRun={dry_run}")

        def run_worker():
            proc = None
            try:
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(ROOT_DIR),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    encoding="utf-8",
                    errors="replace"
                )
                self.process = proc

                for line in iter(proc.stdout.readline, ''):
                    if not line:
                        break
                    clean_line = ANSI_REGEX.sub('', line).strip()
                    self.log(clean_line)
                    self._parse_output_line(clean_line)

                if proc:
                    proc.wait()
            except Exception as e:
                self.log(f"Runner error: {e}")
            finally:
                self.status = "idle"
                self.current_task = None
                self.process = None
                self.log("Auto-Apply process finished.")
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            self.broadcast({
                                "type": "status",
                                "status": "idle",
                                "stats": self.stats
                            }), loop
                        )
                except Exception:
                    pass

        t = threading.Thread(target=run_worker, daemon=True)
        t.start()

    def _parse_output_line(self, line: str):
        """Parse runner stdout to update real-time progress and job cards."""
        # Check for Applied count: e.g. "✔ [LINKEDIN] Applied so far: 3/5" or "✔ [NAUKRI] Applied so far: 4/5"
        app_match = re.search(r'Applied so far:\s*(\d+)/(\d+)', line, re.IGNORECASE)
        if app_match:
            self.stats["applied"] = int(app_match.group(1))

        # Check for Job Header: e.g. "1/50 · Microsoft — Backend Engineer"
        job_match = re.search(r'(\d+/\d+)\s*·\s*([^—–-]+)[—–-]\s*(.+)', line)
        if job_match:
            progress = job_match.group(1)
            company = job_match.group(2).strip()
            title = job_match.group(3).strip()
            self.stats["total_checked"] += 1
            self.current_job = {
                "progress": progress,
                "company": company,
                "title": title,
                "location": "",
                "score": None,
                "status": "Analyzing & Scoring..."
            }
            self._notify_job_update()
            return

        # Check for Panel Job Title: e.g. "Fullstack Developer — AI Governance (naukri)" or "Senior Software Engineer — Google (linkedin)"
        panel_match = re.search(r'^([A-Za-z0-9 +/._#-]{3,60})\s*[—–-]\s*([A-Za-z0-9 +/._&()#-]{2,60})\s*\((?:naukri|linkedin|external|company site)[^)]*\)', line)
        if panel_match:
            title = panel_match.group(1).strip()
            company = panel_match.group(2).strip()
            self.stats["total_checked"] += 1
            self.current_job = {
                "progress": f"{self.stats['total_checked']}",
                "company": company,
                "title": title,
                "location": "",
                "score": None,
                "status": "Evaluating Fit & Scoring..."
            }
            self._notify_job_update()
            return

        # Check for Match Score: e.g. "Match: 85%" or "score 75" or "[score 80]"
        score_match = re.search(r'(?:score|match)[:\s]+(\d+)', line, re.IGNORECASE)
        if score_match:
            score = int(score_match.group(1))
            self.current_job["score"] = score
            self._notify_job_update()

        # Check for Outcome: "→ applied via naukri" or "→ applied via linkedin"
        if "→ applied" in line.lower() or "applied successfully" in line.lower():
            self.current_job["status"] = "✅ Successfully Applied!"
            self._notify_job_update()
        elif "→ skipped" in line.lower() or "low match" in line.lower() or "off stack" in line.lower() or "not your role" in line.lower():
            self.stats["skipped"] += 1
            self.current_job["status"] = "⏭️ Skipped (Criteria mismatch)"
            self._notify_job_update()
        elif "failed" in line.lower() or "error applying" in line.lower():
            self.stats["failed"] += 1
            self.current_job["status"] = "⚠️ Application Failed"
            self._notify_job_update()

    def _notify_job_update(self):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self.broadcast({
                        "type": "job_event",
                        "job": self.current_job,
                        "stats": self.stats
                    }), loop
                )
        except Exception:
            pass

    def stop(self):
        proc = self.process
        if not proc:
            self.status = "idle"
            return

        self.status = "stopping"
        self.log("Stopping runner process...")
        try:
            # If on Windows, kill process tree
            if sys.platform == "win32":
                subprocess.run(
                    f"taskkill /F /T /PID {proc.pid}",
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            else:
                proc.terminate()
        except Exception as e:
            self.log(f"Error while terminating: {e}")
        finally:
            self.status = "idle"
            self.current_task = None
            self.process = None
            self.log("Runner stopped.")


runner_manager = RunnerManager()
