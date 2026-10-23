# CareSync Healthcare Data Engineering Platform

### End-to-End Azure Data Engineering | Metadata-Driven Ingestion | PySpark | Delta Lake

![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-Orchestration-0078D4?logo=microsoftazure)
![ADLS Gen2](https://img.shields.io/badge/ADLS%20Gen2-Data%20Lake-0078D4?logo=microsoftazure)
![Azure Databricks](https://img.shields.io/badge/Azure%20Databricks-Data%20Processing-FF3621?logo=databricks)
![PySpark](https://img.shields.io/badge/PySpark-Transformations-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20Transactions-00A1E0)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-2E8B57)

**CareSync** is an end-to-end healthcare data engineering project built using Microsoft Azure. It demonstrates how healthcare data from multiple source systems can be ingested, stored, transformed, validated, and maintained through a reusable, metadata-driven data pipeline framework.

The platform primarily uses **Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS Gen2), and Azure Databricks** to implement scalable data ingestion and processing workflows.

The solution follows the Medallion Architecture and uses PySpark and Delta Lake to support incremental loading, data quality checks, deduplication, schema evolution, and reliable data processing.

> **Project focus:** Build a reusable metadata-driven framework that minimizes hardcoded pipeline logic and makes onboarding new healthcare datasets easier to maintain and scale.

---

## Table of Contents

- [Business Problem](#-business-problem)
- [Project Objectives](#-project-objectives)
- [Technology Stack](#-technology-stack)
- [Healthcare Data Sources](#-healthcare-data-sources)
- [Solution Architecture](#-solution-architecture)
- [Metadata-Driven Framework](#-metadata-driven-framework)
- [Azure Data Factory Orchestration](#-azure-data-factory-orchestration)
- [ADLS Gen2 Data Lake Design](#-adls-gen2-data-lake-design)
- [Azure Databricks Processing](#-azure-databricks-processing)
- [Medallion Architecture](#-medallion-architecture)
- [Incremental Data Loading](#-incremental-data-loading)
- [Data Quality and Error Handling](#-data-quality-and-error-handling)
- [Schema Drift and Evolution](#-schema-drift-and-evolution)
- [Monitoring and Audit Logging](#-monitoring-and-audit-logging)
- [Security and Governance](#-security-and-governance)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
- [Expected Outcomes](#-expected-outcomes)
- [Future Enhancements](#-future-enhancements)

---

## 🏥 Business Problem

Healthcare organizations generate data across multiple operational systems, including Electronic Health Records (EHR), Hospital Management Systems, Laboratory Information Systems, pharmacy applications, and insurance platforms.

Managing these datasets independently creates several data engineering challenges:

- Healthcare data is distributed across multiple source systems and file formats.
- Patient, encounter, and provider records may contain duplicates or inconsistent identifiers.
- New and updated records must be processed without unnecessarily reloading entire datasets.
- Source schemas can change as healthcare applications evolve.
- Mandatory clinical and financial fields require validation.
- Pipelines must be reusable, configurable, and easy to monitor.
- Sensitive healthcare information requires appropriate security and access controls.
- Downstream analytics requires consistent, validated, and trusted data.

### Proposed Solution

CareSync implements a centralized Azure data platform that uses ADF to orchestrate ingestion, ADLS Gen2 to store data, and Azure Databricks to perform distributed processing.

A metadata-driven framework controls source configuration, load type, target location, primary keys, and processing rules. This allows the same reusable ingestion framework to handle multiple healthcare datasets.

## 🎯 Project Objectives

1. Build an end-to-end Azure Data Engineering solution for healthcare data.
2. Implement metadata-driven ingestion using Azure Data Factory.
3. Store source data and processed datasets in ADLS Gen2.
4. Use Azure Databricks and PySpark for distributed data transformations.
5. Implement Bronze, Silver, and Gold layers using the Medallion Architecture.
6. Support full and incremental data loading.
7. Implement Delta Lake MERGE for inserts and updates.
8. Apply deduplication and data quality validation.
9. Handle controlled schema evolution and source schema drift.
10. Implement reusable notebooks, centralized logging, and error handling.
11. Design for secure access and reliable pipeline execution.

---

## 🛠 Technology Stack

| Technology | Responsibility |
|---|---|
| Azure Data Factory | Pipeline orchestration, parameterization, scheduling, and dependency management |
| Azure Data Lake Storage Gen2 | Centralized landing and data lake storage |
| Azure Databricks | Distributed processing and transformation |
| PySpark | Data cleansing, validation, deduplication, and business transformations |
| Delta Lake | Transactional storage, incremental MERGE, and table history |
| SQL | Metadata management, validation, and transformation logic |
| GitHub | Version control and project collaboration |
| Azure Key Vault | Optional secure management of secrets and credentials |
| Unity Catalog | Optional centralized data governance and access control |

The primary implementation is centered on ADF, ADLS Gen2, and Azure Databricks. Additional services can be introduced as the project evolves.

---

## 📂 Healthcare Data Sources

The project uses healthcare datasets to simulate data arriving from multiple source applications.

| Dataset | Description | Business Key |
|---|---|---|
| Patients | Patient demographics | `patient_id` |
| Encounters | Hospital visits and admissions | `encounter_id` |
| Providers | Doctors and healthcare professionals | `provider_id` |
| Diagnoses | Diagnosis details associated with encounters | `diagnosis_id` |
| Lab Results | Laboratory observations | `lab_result_id` |
| Prescriptions | Medication and prescription details | `prescription_id` |
| Insurance Claims | Claim amounts and processing statuses | `claim_id` |

Possible input formats include CSV and JSON.

All sample datasets should be synthetic or appropriately de-identified. The project is for educational and portfolio purposes and does not represent an actual healthcare provider's production environment.

### Example Business Scenarios

- A new patient record arrives from a registration system.
- An existing patient's address or demographic information changes.
- New hospital encounters arrive every day.
- Laboratory results arrive after an encounter is created.
- An insurance claim changes from submitted to approved.
- A source file contains duplicate records.
- A new source column is introduced.
- An incomplete or invalid record must be quarantined.

These scenarios help demonstrate real-world data engineering requirements.

--

## 🏗️ Solution Architecture

![CareSync Healthcare Data Engineering Solution Architecture](docs/images/caresync-solution-architecture.png)


<img width="1536" height="1024" alt="Healthcare Data Platform Solution Architecture" src="https://github.com/user-attachments/assets/83731476-3e67-49c9-9641-98325c5dd31a" />


### Architecture Highlights

- **Sources:** Patient, encounter, provider, laboratory, prescription, and insurance claim data.
- **Azure Data Factory:** Reads metadata, iterates over datasets, orchestrates ingestion, triggers Databricks notebooks, and monitors execution.
- **ADLS Gen2:** Stores incoming files and organizes data into Bronze, Silver, and Gold zones.
- **Azure Databricks:** Performs cleansing, deduplication, data quality validation, incremental MERGE, and controlled schema evolution.
- **Analytics:** Gold datasets support Power BI, SQL analytics, and operational reporting.
- **Cross-cutting controls:** Security, access management, audit logging, monitoring, and source control.
```

### End-to-End Processing Flow

1. ADF reads active dataset configurations from the metadata source.
2. ADF iterates through configured healthcare datasets.
3. The pipeline identifies the source, format, load strategy, and target.
4. Source data is ingested into the ADLS Gen2 landing zone.
5. ADF invokes the appropriate Databricks notebook with runtime parameters.
6. Databricks validates the input and writes raw data to Bronze Delta tables.
7. Silver processing standardizes, validates, and deduplicates the records.
8. Incremental changes are applied to target Delta tables.
9. Gold datasets are prepared for downstream analytics.
10. Processing status, record counts, and failures are captured in audit logs.

---

## ⚙️ Metadata-Driven Framework

The core design principle is to separate pipeline configuration from pipeline execution logic.

Instead of creating a different hardcoded pipeline for every healthcare dataset, CareSync uses reusable pipelines and Databricks notebooks driven by metadata.

### Example Metadata Configuration

| Column | Example Value | Purpose |
|---|---|---|
| `source_name` | `patients` | Identifies the source dataset |
| `source_system` | `ehr_system` | Identifies the originating system |
| `source_path` | `landing/patients/` | Defines the input location |
| `source_format` | `csv` | Defines the file format |
| `target_table` | `bronze_patients` | Defines the destination table |
| `load_type` | `INCREMENTAL` | Defines the loading strategy |
| `primary_key` | `patient_id` | Defines the business key |
| `watermark_column` | `last_updated_timestamp` | Identifies changed records |
| `is_active` | `true` | Controls whether processing is enabled |

This configuration is illustrative. The actual schema should reflect the metadata tables implemented in the repository.

### How the Framework Works

**Step 1 — Read metadata**

ADF uses a Lookup activity or another supported metadata access pattern to retrieve active dataset configurations.

**Step 2 — Iterate through datasets**

A ForEach activity iterates over the selected configurations.

**Step 3 — Pass parameters**

The pipeline passes dataset-specific parameters to the ingestion activity or Databricks notebook.

Example parameters:

```json
{
  "source_name": "patients",
  "source_format": "csv",
  "load_type": "INCREMENTAL",
  "primary_key": "patient_id",
  "target_table": "bronze_patients"
}
```

**Step 4 — Execute reusable processing logic**

Databricks reads the configuration and applies the corresponding load strategy.

**Step 5 — Record execution results**

The framework records batch status, processing metrics, and errors for each dataset.

### Benefits

- Reduces duplicated pipeline development.
- Simplifies onboarding of new datasets.
- Centralizes ingestion configuration.
- Supports reusable full and incremental load patterns.
- Makes pipeline behavior easier to maintain.
- Enables consistent monitoring and auditing.

---

## 🔄 Azure Data Factory Orchestration

Azure Data Factory is the orchestration layer of CareSync.

### Pipeline Responsibilities

- Retrieve metadata configurations.
- Select active datasets.
- Iterate through datasets using ForEach.
- Parameterize source and target locations.
- Trigger ingestion and Databricks notebook activities.
- Manage dependencies and execution order.
- Configure retry policies and timeouts.
- Capture activity failures and execution details.

### Proposed Pipeline Design

**Master pipeline:** `pl_healthcare_master`

Responsibilities:

1. Read active dataset configurations.
2. Iterate over the configuration records.
3. Invoke the appropriate ingestion pipeline.
4. Execute downstream processing.
5. Track completion status.

**Ingestion pipeline:** `pl_healthcare_ingestion`

Responsibilities:

1. Read source and target parameters.
2. Ingest files into the landing zone.
3. Trigger Bronze processing.
4. Capture ingestion metrics.

**Transformation pipeline:** `pl_healthcare_transformation`

Responsibilities:

1. Trigger Silver transformations.
2. Run data quality checks.
3. Apply incremental MERGE operations.
4. Build Gold datasets where required.

These names describe a proposed design and should be aligned with the actual ADF artifacts in the repository.

---

## 🗄 ADLS Gen2 Data Lake Design

ADLS Gen2 provides centralized storage for incoming source files and processed healthcare datasets.

### Proposed Storage Organization

```text
healthcare-data/
│
├── landing/
│   ├── patients/
│   ├── encounters/
│   ├── providers/
│   ├── lab_results/
│   └── claims/
│
├── bronze/
│   ├── patients/
│   ├── encounters/
│   └── claims/
│
├── silver/
│   ├── patients/
│   ├── encounters/
│   └── claims/
│
├── gold/
│   ├── patient_analytics/
│   ├── encounter_analytics/
│   └── claims_analytics/
│
├── quarantine/
│
└── audit/
```

The layout is illustrative. It can be implemented using separate containers, directory paths, or governed Delta tables, depending on the deployment design.

### Storage Responsibilities

- **Landing:** Incoming files before transformation.
- **Bronze:** Raw or minimally transformed source records.
- **Silver:** Cleansed and validated Delta tables.
- **Gold:** Curated datasets for analytics.
- **Quarantine:** Invalid records and associated failure reasons.
- **Audit:** Processing manifests and operational logs.

Retention and deletion policies should be defined according to the data classification and applicable privacy requirements.

---

## ⚡ Azure Databricks Processing

Azure Databricks performs distributed data processing using PySpark and Delta Lake.

### Common Transformation Responsibilities

- Read CSV and JSON files.
- Apply explicit schemas and type conversions.
- Add ingestion and batch metadata.
- Validate mandatory fields.
- Detect and remove duplicates using business rules.
- Standardize column names and values.
- Handle supported schema changes.
- Write data to Delta tables.
- Apply incremental inserts and updates.
- Capture rejected records and execution metrics.

### Reusable Notebook Design

A metadata-driven implementation can separate responsibilities into reusable notebooks or Python modules:

- `nb_read_metadata`
- `nb_bronze_ingestion`
- `nb_silver_transformation`
- `nb_incremental_load`
- `nb_data_quality`
- `nb_audit_logging`

These are suggested names, not a claim that all such notebooks already exist.

### Example PySpark Transformation

```python
from pyspark.sql import functions as F

def standardize_patient_data(df):
    return (
        df
        .withColumn(
            "patient_id",
            F.trim(F.col("patient_id").cast("string"))
        )
        .withColumn(
            "last_updated_timestamp",
            F.to_timestamp("last_updated_timestamp")
        )
        .withColumn(
            "_processed_timestamp",
            F.current_timestamp()
        )
        .filter(
            F.col("patient_id").isNotNull()
        )
    )
```

This example standardizes a patient identifier, converts the update timestamp, adds processing metadata, and filters records with a missing patient identifier. A production implementation should quarantine invalid records rather than silently discard them.

---

## 🥉 Medallion Architecture

### Bronze Layer

Stores source data with ingestion metadata and minimal transformations.

Key responsibilities:

- Preserve source information for traceability.
- Track source files and batch IDs.
- Support controlled reprocessing.
- Record ingestion timestamps.
- Handle duplicate file arrivals through ingestion tracking.

### Silver Layer

Stores standardized and validated healthcare data.

Key responsibilities:

- Apply data types and column mappings.
- Validate required fields and dates.
- Deduplicate records based on business keys and source ordering.
- Apply incremental MERGE operations.
- Quarantine invalid data.
- Enforce approved schema changes.

### Gold Layer

Stores curated, analytics-ready healthcare datasets.

Possible outputs include:

- `dim_patient`
- `dim_provider`
- `fact_encounter`
- `fact_lab_result`
- `fact_insurance_claim`

Example business metrics include daily admission counts, hospital length of stay, laboratory test volumes, and claim approval rates. Metric definitions should be agreed upon before reporting.

---

## 📈 Incremental Data Loading

CareSync is designed to support incremental processing for datasets that contain reliable change information.

### Watermark-Based Loading

A source timestamp such as `last_updated_timestamp` can identify records changed after the last successfully committed watermark.

The framework should:

1. Retrieve the last committed watermark.
2. Read records changed since that point, including any necessary overlap for late-arriving data.
3. Validate and deduplicate the incoming records.
4. Apply inserts and updates to the target Delta table.
5. Validate the processing outcome.
6. Advance the committed watermark only after successful processing.

### Example Delta Lake MERGE

```sql
MERGE INTO healthcare_silver.patients AS target
USING healthcare_staging.patients AS source
ON target.patient_id = source.patient_id

WHEN MATCHED
  AND source.last_updated_timestamp >
      target.last_updated_timestamp
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *;
```

This example assumes compatible source and target schemas, one selected source record per patient, and a reliable update timestamp. Production code should explicitly specify the required columns and resolve duplicate source keys before executing MERGE.

### Important Considerations

- Handle late-arriving records.
- Prevent duplicate processing on retries.
- Maintain a batch-level audit record.
- Use a deterministic tie-breaking strategy for records with identical timestamps.
- Treat source deletions separately when deletion events or CDC information are available.
- Avoid advancing watermarks after failed or incomplete processing.

---

## ✅ Data Quality and Error Handling

Healthcare data requires validation before being made available for downstream analytics.

### Example Validation Rules

| Rule | Expected Behavior |
|---|---|
| Patient ID is mandatory | Reject or quarantine missing identifiers |
| Encounter ID is mandatory | Reject or quarantine missing encounter keys |
| Admission date is valid | Quarantine invalid dates |
| Discharge date is not before admission | Quarantine inconsistent encounters |
| Business key is unique | Deduplicate using a defined ordering rule |
| Claim amount follows business rules | Flag invalid financial values |
| Related patient exists | Validate patient references where required |

### Error Handling Strategy

- Separate transient infrastructure failures from data quality failures.
- Configure bounded retries for transient failures.
- Capture failed records with reason codes.
- Log notebook exceptions and ADF activity errors.
- Track source, valid, invalid, inserted, and updated record counts.
- Make pipeline reruns idempotent.
- Alert on critical failures and SLA breaches.

---

## 🔀 Schema Drift and Evolution

Healthcare source systems may add fields, rename columns, or change data types.

CareSync should implement controlled schema evolution rather than accepting every source change automatically.

### Handling Approach

1. Compare the incoming schema with the configured schema.
2. Detect added, missing, renamed, and incompatible columns.
3. Apply approved additive changes where appropriate.
4. Use explicit mapping rules for renamed fields.
5. Quarantine or fail incompatible changes.
6. Log schema changes for investigation.
7. Validate downstream transformations after the change.

Delta Lake schema evolution should be enabled only for approved changes and compatible target operations.

---

## 📊 Monitoring and Audit Logging

Each pipeline execution should be traceable from source ingestion through target processing.

### Suggested Audit Fields

- `batch_id`
- `pipeline_name`
- `source_name`
- `target_table`
- `start_timestamp`
- `end_timestamp`
- `status`
- `source_record_count`
- `inserted_record_count`
- `updated_record_count`
- `rejected_record_count`
- `error_message`

### Operational Monitoring

- Monitor ADF pipeline and activity failures.
- Capture Databricks notebook execution results.
- Track data volumes and processing duration.
- Identify duplicate file arrivals.
- Monitor data quality thresholds.
- Maintain batch status and restartability.
- Configure alerts for critical pipeline failures.

---

## 🔐 Security and Governance

Healthcare data may contain sensitive personal and clinical information.

The project should follow privacy-by-design practices:

- Use least-privilege access to data and Azure resources.
- Secure access to ADLS Gen2 and Databricks.
- Store secrets in Azure Key Vault where applicable.
- Use managed identities where supported.
- Apply encryption in transit and at rest.
- Restrict sensitive datasets to authorized users.
- Minimize personally identifiable information.
- Apply appropriate retention and deletion policies.
- Use synthetic or appropriately de-identified data for development.

Azure services alone do not establish regulatory compliance. Compliance depends on the full architecture, configuration, operational controls, and applicable legal requirements.

---

## 📁 Repository Structure

The repository is intended to contain Azure Data Factory artifacts, Databricks processing code, configuration examples, and project documentation.

```text
caresync_adf_repos/
│
├── README.md
│
├── adf/
│   ├── pipeline/
│   ├── dataset/
│   ├── linkedService/
│   └── trigger/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── common/
│   └── scd/
│
├── config/
│   ├── dataset_config.json
│   └── schema_config.json
│
├── sql/
│   ├── create_metadata_tables.sql
│   ├── create_delta_tables.sql
│   └── data_quality_checks.sql
│
├── data/
│   └── sample/
│
├── tests/
│   ├── test_data_quality.py
│   └── test_incremental_load.py
│
└── docs/
    ├── architecture.md
    └── implementation_notes.md
```

This is a suggested structure, not a verified inventory of the current repository. Retain your existing ADF export structure and adapt the proposed folders to match it.

---

## 🚀 Getting Started

### Prerequisites

- An Azure subscription.
- An Azure Data Factory instance.
- An ADLS Gen2 storage account.
- An Azure Databricks workspace.
- Appropriate permissions for the required resources.
- Git and GitHub access.

### Step 1 — Clone the Repository

```bash
git clone https://github.com/nitindoye/caresync_adf_repos.git

cd caresync_adf_repos
```

### Step 2 — Configure Azure Resources

Create or configure the required Azure resources:

- Azure Data Factory.
- ADLS Gen2 storage account and containers.
- Azure Databricks workspace.
- Required identities, permissions, and secret references.

Use your own resource names and environment-specific configuration.

### Step 3 — Configure Source Data

Prepare synthetic patient, encounter, provider, laboratory, and claims datasets.

Upload the sample data to the appropriate ADLS Gen2 landing locations.

### Step 4 — Configure Metadata

Populate the metadata configuration with source paths, formats, load types, business keys, target tables, and watermark columns.

### Step 5 — Import or Configure ADF Pipelines

Configure linked services, datasets, pipelines, parameters, and triggers according to the artifacts available in the repository.

Ensure credentials and environment-specific values are not hardcoded.

### Step 6 — Configure Databricks Processing

Set up the required notebooks, cluster or job compute, storage access, and target Delta tables.

### Step 7 — Execute and Validate

Run the pipeline against the synthetic datasets and verify:

- Ingestion status.
- Bronze record counts.
- Silver validation and deduplication.
- Incremental inserts and updates.
- Audit records.
- Error handling and restart behavior.

---

## 📌 Expected Outcomes

The completed project should demonstrate:

- Reusable metadata-driven ingestion.
- Azure Data Factory orchestration.
- ADLS Gen2 data lake organization.
- Distributed transformations using Azure Databricks and PySpark.
- Bronze, Silver, and Gold data processing.
- Incremental data loading using Delta Lake.
- Data quality validation and deduplication.
- Controlled schema evolution.
- Pipeline monitoring and audit logging.
- Secure data engineering practices.

Performance improvements should be measured through repeatable tests rather than assumed in advance.

---

## 🔮 Future Enhancements

- Add CDC-based ingestion for supported healthcare sources.
- Integrate synthetic FHIR-compatible healthcare data.
- Add event-based ingestion for incoming files.
- Implement automated testing for pipelines and transformations.
- Introduce CI/CD for ADF artifacts and Databricks code.
- Add Unity Catalog governance and data lineage.
- Develop analytics dashboards on Gold-layer datasets.
- Add performance and cost optimization experiments.

---

## 👨‍💻 Author

**Nitin Doye**

**Domain:** Healthcare Data Engineering  
**Cloud Platform:** Microsoft Azure  
**Core Services:** Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks  
**Processing:** PySpark, SQL, Delta Lake  
**Design Pattern:** Metadata-Driven Ingestion and Medallion Architecture

---

*CareSync — Building scalable, reusable, and reliable healthcare data pipelines on Microsoft Azure.*

