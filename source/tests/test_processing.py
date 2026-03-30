import pytest
import numpy as np
from processing_replicas import (
    analyze_seismic_window,
    compute_severity_score,
    EVENT_TYPE_EARTHQUAKE,
    EVENT_TYPE_EXPLOSION,
    EVENT_TYPE_NUCLEAR,
    EVENT_TYPE_NONE,
)

FS = 20
WINDOW = 100

def make_signal(freq_hz: float, fs: int = FS, n: int = WINDOW) -> list:
    """Genera un segnale sinusoidale puro alla frequenza richiesta."""
    t = np.linspace(0, n / fs, n, endpoint=False)
    return list(np.sin(2 * np.pi * freq_hz * t) * 500)  # ampiezza alta per superare threshold

# -------------------------------------------------------
# US-11: classificazione Earthquake (0.5–3.0 Hz)
# -------------------------------------------------------
def test_classify_earthquake():
    signal = make_signal(1.5)
    event_type, freq, _, _ = analyze_seismic_window(signal, fs=FS, threshold=10.0)
    assert event_type == EVENT_TYPE_EARTHQUAKE
    assert 0.5 <= freq < 3.0

# -------------------------------------------------------
# US-12: classificazione Conventional explosion (3.0–8.0 Hz)
# -------------------------------------------------------
def test_classify_explosion():
    signal = make_signal(5.0)
    event_type, freq, _, _ = analyze_seismic_window(signal, fs=FS, threshold=10.0)
    assert event_type == EVENT_TYPE_EXPLOSION
    assert 3.0 <= freq < 8.0

# -------------------------------------------------------
# US-13: classificazione Nuclear-like event (>= 8.0 Hz)
# -------------------------------------------------------
def test_classify_nuclear():
    signal = make_signal(9.0)
    event_type, freq, _, _ = analyze_seismic_window(signal, fs=FS, threshold=10.0)
    assert event_type == EVENT_TYPE_NUCLEAR
    assert freq >= 8.0

# -------------------------------------------------------
# US-10: segnale sotto threshold → nessun evento
# -------------------------------------------------------
def test_noise_gate_no_event():
    signal = [0.001] * WINDOW  # segnale quasi piatto
    event_type, _, _, _ = analyze_seismic_window(signal, fs=FS, threshold=80.0)
    assert event_type == EVENT_TYPE_NONE

# -------------------------------------------------------
# US-14: severity score
# -------------------------------------------------------
def test_severity_score_at_threshold():
    score = compute_severity_score(80.0, 80.0)
    assert score == 50.0  # magnitude == threshold → score 50

def test_severity_score_double_threshold():
    score = compute_severity_score(160.0, 80.0)
    assert score == 100.0  # magnitude == 2x threshold → score 100 (capped)

def test_severity_score_zero_threshold():
    score = compute_severity_score(100.0, 0.0)
    assert score == 0.0  # threshold 0 → score 0 (guard)
