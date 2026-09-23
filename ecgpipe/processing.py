"""Stream preprocessing of one ECG lead: cleaning, beat detection, reduction to heart rate,
discretization, plus the beat-matching used for the veracity check."""
from dataclasses import dataclass

import numpy as np
from scipy.signal import butter, find_peaks, sosfilt, sosfilt_zi

from . import config

REFRACTORY_S = 0.25   # no two beats closer than this (caps HR at 240 bpm)
GUARD_S = 0.3         # beats this close to the buffer edge wait for the next chunk


def detect_beats(x, fs):
    """R-peak indices in a cleaned signal: squared derivative -> moving average -> adaptive threshold."""
    if len(x) < fs:
        return np.array([], dtype=int)
    energy = np.diff(x, prepend=x[0]) ** 2
    w = int(0.12 * fs)
    energy = np.convolve(energy, np.ones(w) / w, mode="same")
    peaks, _ = find_peaks(energy, height=0.3 * np.percentile(energy, 99), distance=int(REFRACTORY_S * fs))
    r = int(0.08 * fs)  # snap each energy peak to the largest deflection nearby
    return np.array([max(p - r, 0) + int(np.argmax(np.abs(x[max(p - r, 0):p + r]))) for p in peaks], dtype=int)


def heart_rate(beats, fs):
    """Median-RR heart rate in bpm, or None if fewer than two beats."""
    if len(beats) < 2:
        return None
    return round(60.0 * fs / float(np.median(np.diff(beats))), 1)


def classify_hr(hr):
    """Discretization: numeric HR -> conceptual label."""
    if hr is None:
        return "unknown"
    if hr < config.BRADY_BPM:
        return "brady"
    if hr > config.TACHY_BPM:
        return "tachy"
    return "normal"


@dataclass
class WindowResult:
    clean: np.ndarray   # filtered samples of this window
    beats: list         # absolute sample indices of beats confirmed in this step
    hr: float | None
    hr_class: str


class PatientStream:
    """Per-patient state kept by the consumer between Kafka messages.

    The band-pass filter is causal (its state carries over from one chunk to the next), so windows
    join seamlessly. Beat detection runs on a rolling context buffer, and beats are confirmed only
    once they are GUARD_S away from the right edge, so no beat is lost or counted twice at chunk borders."""

    def __init__(self, fs):
        self.fs = fs
        self.sos = butter(2, [0.5, 40], btype="bandpass", fs=fs, output="sos")
        self.zi = None
        self.buf = np.zeros(0, dtype=np.float32)
        self.beats = []
        self.confirmed_until = 0

    def push(self, raw, start):
        raw = np.asarray(raw, dtype=np.float32)
        if self.zi is None:
            self.zi = sosfilt_zi(self.sos) * raw[0]
        clean, self.zi = sosfilt(self.sos, raw, zi=self.zi)

        end = start + len(raw)
        self.buf = np.concatenate([self.buf, clean])[-config.CONTEXT_S * self.fs:]
        buf_start = end - len(self.buf)
        edge = end - int(GUARD_S * self.fs)
        refractory = int(REFRACTORY_S * self.fs)

        new = []
        for p in detect_beats(self.buf, self.fs) + buf_start:
            if self.confirmed_until <= p < edge and (not self.beats or p - self.beats[-1] > refractory):
                self.beats.append(int(p))
                new.append(int(p))
        self.confirmed_until = max(self.confirmed_until, edge)
        self.beats = [b for b in self.beats if b >= end - config.CONTEXT_S * self.fs]

        hr = heart_rate(np.array(self.beats), self.fs)
        return WindowResult(clean=clean, beats=new, hr=hr, hr_class=classify_hr(hr))


def match_beats(detected, reference, fs, tol_s=0.15):
    """Sensitivity and positive predictive value of detected beats against reference annotations."""
    detected, reference = np.sort(detected), np.sort(reference)
    if len(detected) == 0 or len(reference) == 0:
        return 0.0, 0.0
    i, last = np.searchsorted(detected, reference), len(detected) - 1
    right, left = detected[np.clip(i, 0, last)], detected[np.clip(i - 1, 0, last)]
    nearest = np.minimum(np.abs(right - reference), np.abs(left - reference))
    tp = int(np.sum(nearest <= tol_s * fs))
    return tp / len(reference), min(tp / len(detected), 1.0)
