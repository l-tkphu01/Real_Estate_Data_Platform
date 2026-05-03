from src.cdc.fingerprint import generate_fingerprint
from src.cdc.state_store import CdcStateStore

# Khởi tạo mock settings để test
from collections import namedtuple
MockCdcSettings = namedtuple("MockCdcSettings", ["id_field", "hash_fields", "hash_algorithm", "state_filename"])
MockSettings = namedtuple("MockSettings", ["cdc"])
mock_settings = MockSettings(cdc=MockCdcSettings(id_field="property_id", hash_fields=["property_id", "title", "price", "area_sqm"], hash_algorithm="md5", state_filename="fingerprints.json"))

class MockStorage:
    def __init__(self):
        self.memory = {}
    def get_json(self, path):
        return self.memory.get(path, {})
    def put_json(self, path, data):
        self.memory[path] = data

def run_test():
    storage = MockStorage()
    store = CdcStateStore(storage_client=storage, state_file=mock_settings.cdc.state_filename)

    print("\n================ NGAY 1: CAO DATA LAN DAU ================")
    records_day1 = [
        {"property_id": "1", "title": "Ban Nha Dep Quan 1", "price": 5000, "area_sqm": 50},
        {"property_id": "2", "title": "Can ho Vinhomes", "price": 3000, "area_sqm": 80}
    ]
    
    passed_day1 = 0
    for r in records_day1:
        pid = str(r[mock_settings.cdc.id_field])
        h = generate_fingerprint(r, mock_settings)
        if store.check_status(pid, h) == "NEW":
            store.update_state(pid, h)
            passed_day1 += 1
            print(f"  [V] Nha ID {pid}: MOI TINH! Qua cua. (hash={h[:12]}...)")
    
    store.save()
    print(f"-> Ket qua: {passed_day1} tin di vao PySpark.\n")


    print("================ NGAY 2: CAO LAI (Sang hom sau) ================")
    records_day2 = [
        {"property_id": "1", "title": "Ban Nha Dep Quan 1", "price": 5000, "area_sqm": 50},
        {"property_id": "2", "title": "Can ho Vinhomes", "price": 3000, "area_sqm": 80}
    ]
    
    passed_day2 = 0
    for r in records_day2:
        pid = str(r[mock_settings.cdc.id_field])
        h = generate_fingerprint(r, mock_settings)
        if store.check_status(pid, h) != "UNCHANGED":
            store.update_state(pid, h)
            passed_day2 += 1
        else:
            print(f"  [X] Nha ID {pid}: TRUNG BAI CU. Vut thung rac. (hash={h[:12]}...)")
            
    store.save()
    print(f"-> Ket qua: Chi {passed_day2} tin di vao PySpark.\n")


    print("================ NGAY 3: CHU NHA DOI GIA & DANG BAI MOI ================")
    records_day3 = [
        {"property_id": "1", "title": "Ban Nha Dep Quan 1", "price": 4500, "area_sqm": 50},
        {"property_id": "2", "title": "Can ho Vinhomes", "price": 3000, "area_sqm": 80},
        {"property_id": "3", "title": "Sang nhuong quan Cafe", "price": 1000, "area_sqm": 120}
    ]
    
    passed_day3 = 0
    for r in records_day3:
        pid = str(r[mock_settings.cdc.id_field])
        h = generate_fingerprint(r, mock_settings)
        if store.check_status(pid, h) != "UNCHANGED":
            store.update_state(pid, h)
            passed_day3 += 1
            print(f"  [V] Nha ID {pid}: CO THAY DOI / MOI! Bom qua PySpark. (hash={h[:12]}...)")
        else:
            print(f"  [X] Nha ID {pid}: CU RICH. Vut thung rac. (hash={h[:12]}...)")
            
    store.save()
    print(f"-> Ket qua: Bom {passed_day3} tin vao PySpark (ID 1 do doi gia, ID 3 do la bai moi).\n")

if __name__ == "__main__":
    run_test()
