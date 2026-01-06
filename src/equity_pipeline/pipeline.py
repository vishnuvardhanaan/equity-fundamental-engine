import logging
import threading
import tkinter as tk
from enum import Enum, auto
from tkinter import ttk

from equity_pipeline.config import PIPELINE_PROFILES
from equity_pipeline.db import init_db
from equity_pipeline.logging_config import setup_logging
from equity_pipeline.run_summary import RunSummary, StepSummary
from equity_pipeline.runner import run_step


class TkLogHandler(logging.Handler):
    def __init__(self, widget):
        super().__init__()
        self.widget = widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        self.widget.after(0, self._append, msg)

    def _append(self, msg):
        self.widget.configure(state="normal")
        self.widget.insert("end", msg)
        self.widget.see("end")
        self.widget.configure(state="disabled")


class PipelineState(Enum):
    IDLE = auto()
    RUNNING = auto()
    DONE = auto()
    FAILED = auto()


class EquityDataPipeline(tk.Tk):
    def __init__(self):
        super().__init__()
        self._build_ui()
        self._setup_logging()
        self.state = PipelineState.IDLE
        self.cancel_event = threading.Event()
        init_db()
        self.run_summary = None

        self.title("Equity Data Pipeline")
        self.geometry("900x600")

    def _build_ui(self):
        tk.Label(
            self,
            text="National Stock Exchange Equity Pipeline",
            font=("Segoe UI", 14, "bold"),
        ).pack(pady=10)

        self.profile = tk.StringVar(value="Choose Data")

        ttk.Label(self, text="Pipeline Profile:").pack()
        ttk.Combobox(
            self,
            textvariable=self.profile,
            values=list(PIPELINE_PROFILES.keys()),
            state="readonly",
            width=20,
        ).pack(pady=5)

        self.run_button = tk.Button(self, text="Run", command=self.start_pipeline)
        self.run_button.pack()

        self.cancel_button = tk.Button(
            self, text="Cancel", state="disabled", command=self.cancel_pipeline
        )
        self.cancel_button.pack()

        self.done_button = tk.Button(
            self, text="Done", state="disabled", command=self.destroy
        )
        self.done_button.pack(pady=5)

        self.progress = ttk.Progressbar(self, maximum=len(PIPELINE_PROFILES))
        self.progress.pack(fill="x", padx=10, pady=10)

        self.step_status = tk.Label(self, text="Idle")
        self.step_status.pack()

        self.log_box = tk.Text(self, height=20, state="disabled")
        self.log_box.pack(fill="both", expand=True)

    def _setup_logging(self):
        setup_logging(TkLogHandler(self.log_box))

    def start_pipeline(self):
        if self.state == PipelineState.RUNNING:
            return
        steps = PIPELINE_PROFILES[self.profile.get()]
        self.progress["maximum"] = len(steps)
        self.progress["value"] = 0
        self._set_state(PipelineState.RUNNING)
        threading.Thread(target=self.run_pipeline, daemon=True).start()

    def cancel_pipeline(self):
        self.cancel_event.set()
        logging.warning("Pipeline Cancellation Requested")

    def run_pipeline(self):

        steps = PIPELINE_PROFILES[self.profile.get()]
        total = len(steps)
        self.run_summary = RunSummary()

        for idx, (name, fn) in enumerate(steps, 1):

            if self.cancel_event.is_set():
                logging.warning("Pipeline cancelled by user before step '%s'", name)
                self.run_summary.finish("CANCELLED")
                self._set_state(PipelineState.FAILED)  # or create CANCELLED state
                return

            step = StepSummary(name)
            self.run_summary.steps[name] = step

            try:
                result = run_step(
                    name,
                    fn,
                    retries=3,
                    backoff=2.0,
                    on_event=self._on_event,
                    cancel_event=self.cancel_event,
                )
                step.attempts = result["attempts"]
                step.duration = result["duration"]
                step.status = "SUCCESS"
            except Exception as exc:
                step.status = "FAILED"
                step.error = str(exc)
                self.run_summary.finish("FAILED")
                self._set_state(PipelineState.FAILED)
                return

            self.after(0, lambda: self.progress.config(value=idx))

        logging.info(
            "Pipeline Profile '%s' Completed (%d steps)",
            self.profile.get(),
            len(steps),
        )

        self.run_summary.finish("SUCCESS")
        self._set_state(PipelineState.DONE)

    def _on_event(self, event, step, *args):
        def update(text):
            if hasattr(self, "step_status"):
                self.step_status.config(text=text)

        if event == "start":
            attempt = args[0] if args else 1
            self.after(0, lambda: update(f"Running {step} (attempt {attempt})"))
        elif event == "failure":
            attempt, elapsed = args
            self.after(
                0,
                lambda: update(
                    f"Retrying {step} after {elapsed:.2f}s (attempt {attempt})"
                ),
            )
        elif event == "success":
            elapsed = args[0]
            self.after(0, lambda: update(f"Completed {step} in {elapsed:.2f}s"))

    def _set_state(self, state):
        self.state = state

        def update_buttons():

            if state == PipelineState.RUNNING:
                self.run_button.config(state="disabled")
                self.cancel_button.config(state="normal")
                self.done_button.config(state="disabled")
            else:
                self.run_button.config(state="normal")
                self.cancel_button.config(state="disabled")
                self.done_button.config(state="normal")

        self.after(0, update_buttons)


def run():
    EquityDataPipeline().mainloop()
