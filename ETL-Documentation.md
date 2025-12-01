Dokumen ETL Architecture – Unit SARPRAS

**1\. Pendahuluan**
-------------------

Proses **Extract, Transform, Load (ETL)** merupakan komponen inti dalam pengembangan Data Warehouse Sarana dan Prasarana. ETL berfungsi untuk mengekstraksi data dari berbagai sumber, melakukan proses transformasi dan validasi, serta memuat data yang telah terstandarisasi ke dalam Data Warehouse.

Arsitektur ETL ini dirancang agar:

*   Data yang digunakan untuk analisis memiliki kualitas tinggi.
    
*   Riwayat perubahan data dapat tersimpan dengan baik.
    
*   Performansi proses pemuatan maupun pengambilan data tetap optimal.
    
*   Sistem mendukung pelacakan, audit, dan pemeliharaan jangka panjang.
    

**2\. Arsitektur Umum ETL**
---------------------------

Secara garis besar, arsitektur ETL dalam Data Warehouse Sarpras terdiri dari tiga lapisan utama, yaitu:

### **a. Source Layer (Lapisan Sumber Data)**

Lapisan ini merupakan tempat penyimpanan data awal yang berasal dari berbagai sistem operasional, antara lain:

*   Sistem pencatatan penggunaan ruangan
    
*   Sistem pergerakan barang
    
*   Sistem perawatan dan perbaikan fasilitas
    
*   File eksternal seperti Excel, CSV, atau database operasional lainnya
    

Data pada lapisan ini belum mengalami proses pembersihan atau standarisasi.

### **b. Staging Layer**

Staging adalah area penampungan sementara yang berfungsi untuk menerima data mentah yang diambil dari sumber.

Karakteristik utama staging:

*   Menyimpan data tanpa modifikasi awal.
    
*   Memungkinkan proses validasi, audit, dan pengecekan anomali.
    
*   Menjadi buffer untuk menghindari gangguan langsung pada Data Warehouse apabila terjadi kesalahan input.
    

Semua tabel staging menggunakan skema:

stg.\*

### **c. Data Warehouse Layer**

Pada tahap ini, data telah melewati proses transformasi, standarisasi, dan integrasi, sehingga siap digunakan untuk keperluan analisis dan Business Intelligence (BI).

Skema penyimpanan menggunakan:

dw.\*

Struktur penyimpanan terbagi menjadi:

*   **Tabel Dimensi (Dim)** Menyimpan entitas referensi seperti unit, lokasi, gedung, ruangan, atau barang.
    
*   **Tabel Fakta (Fact)** Menyimpan data transaksi atau aktivitas yang bersifat terukur, seperti pemakaian ruangan atau perbaikan fasilitas.
    

**3\. Strategi Indexing (Optimasi Akses Data)**
-----------------------------------------------

Agar proses pengambilan data (query) dapat dilakukan secara efisien, indeks digunakan sebagai mekanisme optimasi. Beberapa bentuk indeks yang diterapkan meliputi:

### **a. Clustered Index**

Digunakan untuk mempercepat pencarian berdasarkan kolom yang sering dipakai sebagai dasar query, misalnya:

CREATE CLUSTERED INDEX CIX\_Fact\_RoomUsage\_DateKey

ON dbo.Fact\_RoomUsage(DateKey, RoomUsageKey);

### **b. Nonclustered Index (Covering Index)**

Digunakan untuk menghindari pembacaan seluruh tabel ketika query hanya membutuhkan kolom tertentu.

Contoh:

CREATE NONCLUSTERED INDEX IX\_Fact\_Repair\_Covering

ON dbo.Fact\_Repair(ItemKey, DateKey)

INCLUDE (DaysToComplete, Status);

### **c. Columnstore Index**

Digunakan untuk mendukung workload analitis berskala besar sehingga proses pemindaian data menjadi lebih cepat.

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCIX\_Fact\_RoomUsage

Penerapan indeks yang tepat meningkatkan performansi keseluruhan Data Warehouse, terutama pada dashboard dan laporan analitis.

**4\. Strategi Partitioning (Partisi Data per Tahun)**
------------------------------------------------------

Tabel fakta biasanya memiliki jumlah data yang sangat besar dan terus bertambah setiap tahun. Untuk menjaga performansi query, tabel fakta dipartisi berdasarkan kolom DateKey.

Contoh definisi fungsi partisi:

CREATE PARTITION FUNCTION PF\_DateKey\_Year

AS RANGE RIGHT FOR VALUES (2020, 2021, 2022, 2023, 2024, 2025);

Manfaat partisi:

*   Mempercepat proses query yang hanya membaca sebagian data.
    
*   Memudahkan aktivitas pemeliharaan seperti archive atau purge.
    
*   Mengurangi beban pemrosesan pada saat pemuatan data.
    

Seluruh tabel fakta besar, seperti:

*   Fact\_RoomUsage
    
*   Fact\_FacilityRequest
    
*   Fact\_ItemMovement
    

dikelola menggunakan mekanisme partisi ini.

**5\. Perancangan Staging**
---------------------------

Setiap sumber data operasional memiliki tabel staging tersendiri yang berfungsi sebagai area penerimaan data awal.

Tabel staging selalu memiliki:

*   Data mentah tanpa modifikasi
    
*   Kolom SourceSystem
    
*   Kolom LoadDate
    

Hal ini memungkinkan:

*   Pelacakan asal data
    
*   Audit proses ETL
    
*   Pemrosesan ulang apabila terjadi kesalahan tanpa mengganggu Data Warehouse
    

Contoh nama tabel:

stg.Dim\_Date  

stg.Fact\_RoomUsage

**6\. Transformasi dan Pemanggilan ETL (Stored Procedure)**
-----------------------------------------------------------

Seluruh proses transformasi dilakukan melalui _stored procedure_ yang disusun sesuai kebutuhan masing-masing tabel.

### **a. Dimensi**

Tabel dimensi menggunakan pendekatan **Slowly Changing Dimension (SCD) Type 2**, dimana:

*   Perubahan data menghasilkan versi baru (record baru).
    
*   Riwayat data tetap tersimpan dan dapat ditelusuri.
    
*   Record aktif ditandai sebagai IsCurrent = 1.
    

Contoh eksekusi:

EXEC etl.Load\_Dim\_Room;

### **b. Tabel Fakta**

Tabel fakta umumnya dimuat dengan pendekatan insert langsung setelah relasi valid.

Contoh:

EXEC etl.Load\_Fact\_RoomUsage;

### **c. Eksekusi ETL Terjadwal**

Seluruh proses dapat dijalankan melalui satu prosedur master:

EXEC etl.Master\_ETL\_Sarpras;

Dengan demikian, pemuatan data dilakukan secara terstruktur, berurutan, dan terkontrol.

**7\. Data Quality Assurance (DQA)**
------------------------------------

Pemeriksaan kualitas data merupakan bagian penting dalam setiap siklus ETL. Validasi dilakukan untuk memastikan bahwa data yang dimuat memenuhi persyaratan integritas dan kelengkapan, antara lain:

### **a. Validasi nilai NULL**

Contoh pengecekan:

SELECT COUNT(\*) AS TotalRows,

       SUM(CASE WHEN NamaUnit IS NULL THEN 1 ELSE 0 END) AS Null\_NamaUnit

FROM dw.Dim\_Unit;

### **b. Pengecekan Konsistensi dan Hubungan Antar-Entitas**

Meliputi:

*   Ketersediaan foreign key
    
*   Integritas referensial
    
*   Validitas enumerasi nilai
    

DQA dilakukan pada setiap tabel dimensi dan fakta, seperti:

*   Dim\_Room
    
*   Dim\_Item
    
*   Dim\_Unit
    
*   Fact\_RoomUsage
    
*   Fact\_ItemMovement
    

**8\. Alur Kerja ETL Secara Keseluruhan**
-----------------------------------------

Secara sistematis, alur kerja ETL dapat dijelaskan sebagai berikut:

1.  Sistem operasional menghasilkan data baru.
    
2.  Data dikumpulkan dan dimuat ke tabel staging.
    
3.  ETL melakukan:
    
    *   pembersihan dan standarisasi,
        
    *   validasi struktur dan isi data,
        
    *   pemuatan ke Data Warehouse.
        
4.  Proses indexing dan partitioning memastikan performansi tetap optimal.
    
5.  Data Quality Assurance memverifikasi kelayakan data sebelum digunakan untuk analisis.
    

**9\. Kesimpulan**
------------------

Arsitektur ETL ini dirancang untuk menjamin:

*   Ketersediaan data yang konsisten, terintegrasi, dan akurat.
    
*   Pengelolaan data historis yang terstruktur melalui pendekatan SCD.
    
*   Performansi query yang optimal melalui penggunaan indeks dan partisi.
    
*   Monitoring kualitas data melalui proses Data Quality Assurance yang sistematis.
    

Dengan demikian, Data Warehouse mampu mendukung kebutuhan analitik, pelaporan, dan pengambilan keputusan secara berkelanjutan.

