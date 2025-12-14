# Demo Queries

Queries use boosted multi-field search across title, abstract, keywords, authors, and full text:
```python
SEARCH_FIELDS = [
    "title^4",      # Highest boost
    "abstract^3",   # High boost
    "keywords^3",   # High boost
    "authors^2",    # Medium boost
    "full_text",    # No boost
]
```

**Index stats:** 620 documents, 427 with abstracts, 578 with authors, 425 with DOIs

---

## Query 1: "deduplication" (basic search)
```bash
pdf-ingest search -q "deduplication" --size 5
```

```
  9.39  2012  Secure and efficient proof of storage with deduplication
        venue=Proceedings of the second ACM conference on Data and Application Security and Privacy tags=['PoW', 'Secure Dedup']

  9.39  2014  A tunable proof of ownership scheme for deduplication using Bloom filters
        venue=2014 IEEE Conference on Communications and Network Security tags=['PoW']

  9.39  2015  Secure Deduplication of Encrypted Data without Additional Independent Servers
        venue=Proceedings of the 22nd ACM SIGSAC Conference on Computer and Communications Security tags=['Secure Dedup']

  9.39  2014  A Survey and Classification of Storage Deduplication Systems
        venue=ACM Comput. Surv. tags=['Secure Dedup', 'Survey', 'Super-chunking']

  9.39  2020  A similarity clustering-based deduplication strategy in cloud storage systems
        venue=2020 IEEE 26th International Conference on Parallel and Distributed Systems tags=['Similarity/Resemblance']
```

---

## Query 2: "content-defined chunking" (title + abstract boost)
```bash
pdf-ingest search -q "content-defined chunking" --size 5
```

```
 37.63  2010  Bimodal content defined chunking for backup streams
        venue=Fast tags=['Chunking']

 35.54  2020  Function of Content Defined Chunking Algorithms in Incremental Synchronization
        venue=IEEE Access tags=['Chunking']

 33.67  2016  Throughput: A Key Performance Measure of Content-Defined Chunking Algorithms
        venue=2016 IEEE 36th International Conference on Distributed Computing Systems Workshops tags=['Chunking', 'Metric/Measure Deduplication']

 31.98  2016  Fastcdc: a fast and efficient content-defined chunking approach for data deduplication
        venue=2016 USENIX Annual Technical Conference tags=['Chunking']

 31.98  2017  A new content-defined chunking algorithm for data deduplication in cloud storage
        venue=Future Gener. Comput. Syst. tags=['Chunking']
```

**Notes:** Higher scores (37.63 vs 9.39) due to title matches getting 4x boost

---

## Query 3: "FSL" (grep with context snippets)
```bash
pdf-ingest grep -q "FSL" --size 3 --fragments 2
```

```
================================================================================
 16.39  2021  Improving the Performance of Deduplication-Based Backup Systems via Container Utilization Based Hot Fingerprint Entry Distilling

    ...Since the >>>FSL<<< trace does not have the byte
stream of the data chunks, we cannot evaluate the actual throughput on the >>>FSL<<< trace like on the
Linux trace...

    ...Speed factor with the >>>FSL<<< dataset.
slightly outperforms EHID....

================================================================================
  5.16  2019  Metadedup: Deduplicating Metadata in Encrypted Deduplication via Indirection

    ...In addition to
>>>FSL<<< and VM, we include the MS dataset for cross-dataset
validation....

    ...The metadata storage saving of the MS dataset is lower
than that of the >>>FSL<<< and VM datasets....

================================================================================
  5.16  2018  A simulation analysis of redundancy and reliability in primary storage deduplication

    ...Duplicate files are also common in the >>>FSL<<< dataset....

    ...We first study the reliability in the >>>FSL<<< dataset (see Fig-
ure 8). Figure 8(a) shows the non-weighted chunk-level reli-
ability in the >>>FSL<<< dataset....
```

**Notes:** FSL is a standard benchmark dataset for deduplication research

---

## Query 4: "secure storage encryption" (multi-field search)
```bash
pdf-ingest search -q "secure storage encryption" --size 5
```

```
 21.16  2013  ClouDedup: Secure Deduplication with Encrypted Data for Cloud Storage
        venue=2013 IEEE 5th International Conference on Cloud Computing Technology and Science tags=['Secure Dedup', 'CE/MLE']

 20.05  2017  Decentralized Server-aided Encryption for Secure Deduplication in Cloud Storage
        venue=IEEE Trans. Serv. Comput. tags=['Secure Dedup']

 19.05  2022  Secure and Lightweight Deduplicated Storage via Shielded Deduplication-Before-Encryption
        venue=2022 USENIX Annual Technical Conference (USENIX ATC 22) tags=[]

 19.05  2018  Attribute-Based Storage Supporting Secure Deduplication of Encrypted Data in Cloud
        venue=International Journal of Trend in Scientific Research and Development tags=['Secure Dedup']

 17.87  2013  Message-Locked Encryption and Secure Deduplication
        venue=Advances in Cryptology - EUROCRYPT 2013 tags=['CE/MLE', 'Secure Dedup', 'Sec Techniques/Protocols']
```

---

## Query 5: Tag-based search
```bash
pdf-ingest search -q "" --tag "Chunking" --count
```

```
60
```

60 papers tagged with "Chunking" in Paperpile

---

## Query 6: Year filter
```bash
pdf-ingest search -q "neural network" --year-from 2020 --size 5
```

```
 21.29  2022  Cross-domain Resemblance Detection based on Meta-learning for Cloud Storage
        venue=2022 IEEE International Performance, Computing, and Communications Conference tags=[]

 19.08  2025  One-minute video generation with test-Time Training
        venue=None tags=[]

 18.62  2021  Chunk Content is not Enough: Chunk-Context Aware Resemblance Detection for Deduplication Delta Compression
        venue=None tags=['Similarity/Resemblance']

 14.89  2020  A secure data deduplication system for integrated cloud-edge networks
        venue=J. Cloud Comput. Adv. Syst. Appl. tags=[]

 12.86  2022  LOFS: A Lightweight Online File Storage Strategy for Effective Data Deduplication at Network Edge
        venue=IEEE Trans. Parallel Distrib. Syst. tags=[]
```

---

## Query 7: Combined filters (year + tag)
```bash
pdf-ingest search -q "encrypted" --tag "Secure Dedup" --year-from 2018 --size 5
```

**Notes:** Combines full-text search with Paperpile tags and year filtering
