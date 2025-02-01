# Data Migration Pipeline: MongoDB to MySQL

## Overview
This code enables seamless data migration from a MongoDB database to MySQL using a pipeline (Main Script). The process ensures that data is correctly restructured and transferred between the two databases, maintaining integrity and proper formatting.

## Version 0.0
This is the first functional version of the code, designed specifically for JSON files. While it is optimized for JSON, it also has the capability to read and store data from CSV files.

## Future Updates
Please note that this pipeline model is still under development. It will be refactored, modified, and updated by the creator to improve functionality and performance.

## How to Use

- **Assets Folder:**  
  The "assets" folder can be used as a location for handling eventual "local file procedures." For example, you can extend the code to save files locally.
  
- **Test Data:**  
  The "pomodoro" file is a simple test collection used for migration trials.

- **Columns Configuration:**  
  Make sure to manually adjust the **COLUMNS** in the **MAIN script**. The column definitions must match the **Object-Column-Type and Length** correctly, or the code may truncate data during migration.

- **Handling MongoDB IDs:**  
  Every MongoDB object has a unique `_id` field, which will be stored as a **hexadecimal string (VarChar 36)** in MySQL. This field is required for migration and can be kept as the **primary key**. If desired, you can change the primary key after the migration is complete.
