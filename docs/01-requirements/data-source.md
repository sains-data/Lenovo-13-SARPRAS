### Step 2: Identifikasi Sumber Data (Data Source Identification)

**1. Sumber Data Utama**
* Sistem Manajemen Ruang (internal kampus)
* Data Inventaris Sarpras (Excel/Database internal)
* Formulir pengajuan perbaikan
* Formulir permintaan fasilitas baru
* Dokumen mutasi barang

**2. Data Source Analysis**

**Tabel 2.1**
| Sumber Data | Tipe | Tabel Utama | Volume | Frekuensi Update |
| :--- | :--- | :--- | :--- | :--- |
| Sistem Ruang | SQL Server | Rooms, RoomUsage | ~10k | Harian |
| Inventaris Barang | Excel/DB | Items, ItemStatus | ~50k | Mingguan |
| Pengajuan Perbaikan | Form Online | RepairRequest | ~5k | Harian |
| Permintaan Fasilitas | Form Online | FacilityRequest | ~2k | Mingguan |
| Mutasi Barang | Excel | ItemMovement | kecil | Tidak tentu |

Pada tabel 2.1 sistem data terdiri dari lima sumber utama yang digunakan untuk mendukung pengelolaan sarana dan prasarana. Sistem Ruang menggunakan SQL Server sebagai basis data utama, dengan tabel Rooms dan RoomUsage yang menyimpan sekitar 10.000 data dan diperbarui setiap hari untuk memastikan informasi penggunaan ruangan selalu terbaru. Selanjutnya, Inventaris Barang tersimpan dalam format Excel maupun database, mencakup tabel Items dan ItemStatus dengan volume data cukup besar, yaitu sekitar 50.000 baris, dan diperbarui setiap minggu. Untuk Pengajuan Perbaikan, data berasal dari formulir online dan disimpan dalam tabel RepairRequest dengan jumlah sekitar 5.000 data yang diperbarui secara harian karena sifatnya yang operasional dan membutuhkan respons cepat. Permintaan Fasilitas Baru juga dikumpulkan melalui formulir online dan dicatat dalam tabel FacilityRequest, dengan volume sekitar 2.000 data dan diperbarui mingguan. Terakhir, Mutasi Barang dicatat melalui file Excel pada tabel ItemMovement, dengan jumlah data relatif kecil dan frekuensi yang tidak menentu, tergantung ada atau tidaknya perpindahan barang.

**3. Data Profiling**
* Periksa NULL pada RoomCode, ConditionStatus
* Pastikan kapasitas ruang adalah angka positif
* Cek duplikasi pada ItemID dan RoomUsageID
* Konsistensi tanggal pemeliharaan
* Validasi relasi: item.room_id → rooms.room_id

---


