import numpy as np
import pytest

from ecgpipe.processing import PatientStream, classify_hr, heart_rate, match_beats

FS = 250


def synthetic_ecg(bpm, seconds, seed=0):
    """Gaussian 'QRS' spikes at a fixed rate + baseline wander + noise."""
    rng = np.random.default_rng(seed)
    t = np.arange(seconds * FS) / FS
    beats = np.arange(0.5, seconds - 0.5, 60 / bpm)
    x = sum(1.2 * np.exp(-((t - b) ** 2) / (2 * 0.012 ** 2)) for b in beats)
    x += 0.4 * np.sin(2 * np.pi * 0.2 * t) + 0.03 * rng.standard_normal(len(t))
    return x.astype(np.float32), (beats * FS).astype(int)


def stream(x, chunk=FS):
    s, detected, last = PatientStream(FS), [], None
    for start in range(0, len(x), chunk):
        last = s.push(x[start:start + chunk], start)
        detected += last.beats
    return np.array(detected), last


@pytest.mark.parametrize("bpm", [45, 72, 130])
def test_stream_detects_every_beat_and_hr(bpm):
    x, truth = synthetic_ecg(bpm, 60)
    detected, last = stream(x)
    end = len(x) - FS  # score both sides on the same span
    se, ppv = match_beats(detected[detected < end], truth[truth < end], FS)
    assert se > 0.98 and ppv > 0.98
    assert last.hr == pytest.approx(bpm, rel=0.03)


def test_no_double_counting_at_chunk_borders():
    # 0.5 s chunks put many beats right on a border; the count must still match
    x, truth = synthetic_ecg(80, 40)
    detected, _ = stream(x, chunk=FS // 2)
    assert len(np.unique(detected)) == len(detected)
    assert abs(len(detected) - len(truth)) <= 2


def test_discretization():
    assert [classify_hr(v) for v in (None, 45, 60, 100, 101)] == ["unknown", "brady", "normal", "normal", "tachy"]


def test_heart_rate_needs_two_beats():
    assert heart_rate(np.array([100]), FS) is None
    assert heart_rate(np.array([0, 250, 500]), FS) == 60.0


def test_match_beats_counts_misses_and_false_alarms():
    ref = np.array([100, 350, 600, 850])
    se, ppv = match_beats(np.array([102, 355, 700, 849, 1000]), ref, FS)
    assert se == 0.75 and ppv == 0.6
