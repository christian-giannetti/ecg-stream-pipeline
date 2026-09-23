from ecgpipe.dataset import parse_header

HEADER = """e0103 2 250 1800000
e0103.dat 212 200 12 0 91 56457 0 V4
e0103.dat 212 200 12 0 751 48959 0 MLIII
#Age: 62  Sex: M
#Mixed angina
#1-vessel disease (RCA)
#Medications: nitrates, diltiazem
#Recorder type: ICR 7200
"""


def test_header_becomes_patient_document(tmp_path):
    (tmp_path / "e0103.hea").write_text(HEADER)
    doc = parse_header("e0103", data_dir=tmp_path)
    assert doc == {
        "_id": "e0103", "fs": 250, "leads": ["V4", "MLIII"], "duration_s": 7200.0,
        "age": 62, "sex": "M",
        "diagnoses": ["Mixed angina", "1-vessel disease (RCA)"],
        "medications": ["nitrates", "diltiazem"],
        "recorder": "ICR 7200",
    }
