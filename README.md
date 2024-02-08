# SynapseSQLPool-DynamicView

## Purpose

The primary purpose of this solution is to make the work done in cost effective manner. The goal can be achieved simply by using `Stored Procedure` activity in pipeline, but `Stored Procedure` activity is not supported in Synapse Serverless SQL Pool. So, if we use Dedicated SQL Pool, then it will be a costly approach.

## Scope and Goals

The scope is to create views of files that are stored in ADLS Gen2. But catch here is, the views should be created in such a way that the date in file should be dynamic. The goal is to create an solution that can automatically create view in Synapse Serverless SQL Pool, whenever new files arrives in ADLS Gen2.


## Used Technologies
- Azure Synapse


## Steps

### A. Get the latest added file in folder 

Inside the Data Lake, I have a folder in a container that basically contains the files pushed by external source every day. However, I wanted to process only the latest added file in that folder. For this:-
1. Create a Dataset pointing to that folder.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/655d9081-de31-4c90-9dd2-9036709952a2)
   
2. Create another Dataset that will going to point to the files in that folder.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/7df53c35-48c9-43a9-a629-2e4fca9ad72c)

3. Create two variables in pipeline:
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/2915e783-3c67-40f0-b83e-04403c001917)

4. Use `Get Metadata` activity, to get the metadata of that folder.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/edcb60cc-26a4-4f4d-8e68-bc5c9553c7d0)

5. Then, use `ForEach` activity to iterate over all files in that folder.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/4bee1b18-4784-4698-828f-3dca19e01c29)
   
6. In that `ForEach` activity, again add `Get Metadata` activity for files.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/b75399fe-b08f-46f3-aca0-4316f9dfc00e)

7. Then add `If Condition` activity to check the latest file. I have used this dynamic expression because, my filename is like that `<filename_yyyy-mm-dd>` and I checking the latest file based on filename.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/522fd1ce-868b-4af5-836d-8c57c5ec68cb)

8. Then add `Set Variable` activity in `True activities` of `If Condition`. To set the latest filename and maxtime variable.
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/4df3b80c-2f43-4513-8386-1128401267b8)
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/c784963c-ea9e-4ec3-8348-0c95a5b3e499)



### B. Create the View in Azure Synapse Serverless SQL Pool  

1. Create a Linked Service to **Azure Synapse Analytics**.
   - But, there’s one problem however: it expects `Dedicated SQL pools` and doesn’t display the databases of `Synapse Serverless SQL Pool` in the list.
   - As a work around, you can type your Workspace SQL endpoint manually.
  
2. Then in pipeline add `Script` activity to execute the CREATE VIEW command in Synapse Serverless SQL Pool.
   - Enter this code in the Script: 
   ```sql
   DECLARE @sql NVARCHAR(MAX);
   
   SET @sql = N'CREATE VIEW ' + QUOTENAME(@viewName) + N' AS 
   SELECT * FROM OPENROWSET(
   BULK ' + QUOTENAME(@filename, '''') + N',
   DATA_SOURCE = ''raw'',
   FORMAT = ''CSV'', 
   FIELDTERMINATOR ='','', 
   ROWTERMINATOR = ''\n'',
   PARSER_VERSION = ''2.0''
   ) AS [r]';

   EXEC sp_executesql @sql; 
   ```
   **NOTE: -** Some points to be noted in this code is, SQL Server does not support the use of variables or parameters for the view name directly in a CREATE VIEW statement like commands, this is because these names must be a constant. However, it can be achieve by using dynamic SQL (construct the SQL command as a string and then execute it).
   - Enter this Dynamic Expression in the `viewName` parameter:
   ```
   @substring(variables('filename'), 0, indexOf(variables('filename'), '.'))
   ```
   - Enter this Dynamic Expression in the `fileName` parameter:
   ```
   @concat('dynamic_view_files/', variables('filename'))
   ```
   ![image](https://github.com/ayush9892/SynapseSQLPool-DynamicView/assets/85745368/69d6ca9a-8881-4380-a681-135a8f883f0c)
