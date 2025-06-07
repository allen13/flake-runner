# File Orchestration Flow Diagram

This diagram shows the simplified flow of how a provided S3 file moves through the orchestration system to Snowflake loading.

```mermaid
flowchart TD
    Start([S3 File Path Provided]) --> Processing[File Processing & Validation]
    Processing --> ControlCheck{Control File Found?}
    
    ControlCheck -->|Yes| Validation[Data Integrity Validation]
    ControlCheck -->|No| Failed[Mark as Failed]
    
    Validation --> ValidationResult{Validation Passed?}
    ValidationResult -->|Yes| Staging[Prepare for Processing]
    ValidationResult -->|No| Retry{Retry?}
    
    Retry -->|Yes| Validation
    Retry -->|No| Failed
    
    Staging --> EMRProcessing[EMR Serverless Processing]
    EMRProcessing --> EMRResult{EMR Success?}
    
    EMRResult -->|Yes| SnowflakeLoad[Load to Snowflake]
    EMRResult -->|Failed| EMRRetry{Retry EMR?}
    
    EMRRetry -->|Yes| EMRProcessing
    EMRRetry -->|No| Failed
    
    SnowflakeLoad --> LoadResult{Load Success?}
    LoadResult -->|Yes| Cleanup[Cleanup & Complete]
    LoadResult -->|Failed| SnowflakeRetry{Retry Load?}
    
    SnowflakeRetry -->|Yes| SnowflakeLoad
    SnowflakeRetry -->|No| Failed
    
    Cleanup --> Success([Processing Complete])
    Failed --> DeadLetter[Dead Letter Queue]
    
    %% State tracking in DynamoDB
    Processing -.-> DynamoDB[(DynamoDB State Tracking)]
    Validation -.-> DynamoDB
    Staging -.-> DynamoDB
    EMRProcessing -.-> DynamoDB
    SnowflakeLoad -.-> DynamoDB
    Cleanup -.-> DynamoDB
    Failed -.-> DynamoDB
    
    %% AWS Services
    EMRProcessing -.-> EMR[EMR Serverless]
    SnowflakeLoad -.-> Snowflake[(Snowflake)]
    Processing -.-> S3[S3 Storage]
    
    %% Styling
    classDef processNode fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef decisionNode fill:#fff3e0,stroke:#f57c00,stroke-width:2px
    classDef errorNode fill:#ffebee,stroke:#d32f2f,stroke-width:2px
    classDef successNode fill:#e8f5e8,stroke:#388e3c,stroke-width:2px
    classDef awsService fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    
    class Processing,Validation,Staging,EMRProcessing,SnowflakeLoad,Cleanup processNode
    class ControlCheck,ValidationResult,EMRResult,LoadResult,Retry,EMRRetry,SnowflakeRetry decisionNode
    class Failed,DeadLetter errorNode
    class Success successNode
    class DynamoDB,EMR,Snowflake,S3 awsService
```

## Simplified Flow Overview

### 🔄 **Main Processing Stages:**
1. **File Processing & Validation** - Process provided S3 file path and validate control files
2. **Data Integrity Validation** - Verify file integrity using control file metadata
3. **Staging** - Prepare files for EMR processing
4. **EMR Processing** - Execute PySpark job on EMR Serverless
5. **Snowflake Loading** - Load processed data into Snowflake
6. **Cleanup & Complete** - Clean up resources and mark as complete

### ⚠️ **Error Handling:**
- **Retry Logic** - Automatic retries at validation, EMR, and Snowflake stages
- **Dead Letter Queue** - Failed files that exceed retry limits
- **State Persistence** - All stages tracked in DynamoDB

### 🎯 **Key Benefits:**
- **Simplified View** - Focus on main processing stages
- **Clear Decision Points** - Easy to understand success/failure paths
- **DynamoDB Integration** - State tracking throughout the flow
- **AWS Service Integration** - Clear connections to EMR and Snowflake
- **Event-Driven Processing** - Triggered by specific S3 file paths
