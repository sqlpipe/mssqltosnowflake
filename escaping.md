Queries

- SOURCE - Get schemas and tables from source
  - Outputs list of queries to run on source


- Cleanup (quote if needed)
  - `cleanedSourceDBName`
  - `cleanedDraftProdSchemaName`
  - `stagingSchemaName`


- TARGET - Call sp_grant_schema_access (creates prod schema if not exists, returns name)
  - INPUTS
    - Target DB Name
    - Rootname
    - `cleanedSourceDBName`
    - `cleanedDraftProdSchemaName`
  - OUTPUTS
    - `finalProdSchemaName`


- Drop `stagingSchemaName` if exists in target

- Create `stagingSchemaName`

- TARGET - Get DB in `stagingSchemaName`

- TARGET - Create or replace file format in `stagingSchemaDb`

For Table
  - Create `cleanedTableName` (no spaces or non alnum)
  - Create `s3DirName` (clean string, no spaces or non alnum)

  - SOURCE - Get transfer rows

  - TARGET - Create `cleanedTableName` in - `stagingSchemaName`
    
  - S3 - Upload and transfer (make sure s3 dir is `cleanedTableName`)

  - TARGET - Copy into query
    - INPUTS
      - `stagingSchemaName`
      - `cleanedTableName`
      - 

  - TARGET - Drop prod table


  - TARGET - Move staging to prod

- TARGET - Drop staging schema