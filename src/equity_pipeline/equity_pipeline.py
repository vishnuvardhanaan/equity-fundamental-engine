import logging
import threading
import tkinter as tk
from tkinter import ttk

# IMPORT YOUR PIPELINE STEPS
from equity_pipeline.ingestion import equity_info, equity_statements, equity_universe

PIPELINE_STEPS = [
    ("Equity Universe", equity_universe.main),
    ("Equity Info", equity_info.main),
    ("Equity Statements", equity_statements.main),
]


class TkLogHandler(logging.Handler):
    def __init__(self, text_widget: tk.Text):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record) + "\n"
        self.text_widget.after(0, self._append, msg)

    def _append(self, msg):
        self.text_widget.configure(state="normal")
        self.text_widget.insert("end", msg)
        self.text_widget.see("end")
        self.text_widget.configure(state="disabled")


class EquityDataPipeline(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("NSE Equity Universe Data Pipeline")
        self.geometry("900x600")

        self._build_ui()
        self._setup_logging()

    def _build_ui(self):
        tk.Label(
            self,
            text="Run NSE Equity Universe Data Pipeline",
            font=("Segoe UI", 14, "bold"),
        ).pack(pady=15)

        self.run_button = tk.Button(
            self,
            text="Run",
            width=20,
            height=2,
            command=self.start_pipeline,
        )
        self.run_button.pack(pady=10)

        self.progress_label = tk.Label(self, text="Progress: 0 / 3")
        self.progress_label.pack()

        self.progress = ttk.Progressbar(
            self,
            length=600,
            maximum=len(PIPELINE_STEPS),
            mode="determinate",
        )
        self.progress.pack(pady=10)

        self.log_box = tk.Text(self, wrap="word", height=25)
        self.log_box.pack(fill="both", expand=True, padx=10, pady=10)
        self.log_box.configure(state="disabled")

        self.done_button = tk.Button(
            self,
            text="Done",
            width=15,
            state="disabled",
            command=self.destroy,
        )
        self.done_button.pack(pady=10)

    def _setup_logging(self):
        handler = TkLogHandler(self.log_box)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        )

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(handler)

    def start_pipeline(self):
        self.run_button.config(state="disabled")
        threading.Thread(target=self.run_pipeline, daemon=True).start()

    def run_pipeline(self):
        total = len(PIPELINE_STEPS)

        for idx, (name, step_fn) in enumerate(PIPELINE_STEPS, start=1):
            logging.info("=== Running %s ===", name)

            try:
                step_fn()
            except Exception:
                logging.exception("Pipeline failed at %s", name)
                self.done_button.config(state="normal")
                return

            self.progress["value"] = idx
            self.progress_label.config(text=f"Progress: {idx} / {total}")

        logging.info("Pipeline completed successfully")
        self.done_button.config(state="normal")


if __name__ == "__main__":
    EquityDataPipeline().mainloop()
