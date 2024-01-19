       

### <a id="documentation-body"></a>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image1.png?raw=true)

Couchbase Physical Model
------------------------

#### Schema for:

Model name: NWP and obs data

Author:

Version:

File name: Couchbase model RE.json

File path: /Users/randy.pierce/PycharmProjects/VXingest/model/Couchbase model RE.json

Printed On: Wed Sep 22 2021 12:56:36 GMT-0600 (Mountain Daylight Time)

Created with: [Hackolade](https://hackolade.com/) - Visual data modeling for NoSQL and multimodel databases

### <a id="contents"></a>

*   [1. Model](#model)
*   [2. Buckets](#containers)
    *   [2.1 mdata](#c1ca0f70-ac1f-11ea-b509-dba22c0df611)
        
        [2.1.2. Document kinds](#c1ca0f70-ac1f-11ea-b509-dba22c0df611-children)
        
        [2.1.2.1 DataDocument](#df2f8770-acba-11ea-9dd6-9b3aa12441f0)
        
        [2.1.2.2 DataFile](#49dd5370-acae-11ea-9dd6-9b3aa12441f0)
        
        [2.1.2.3 DataSource](#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0)
        
        [2.1.2.4 Lineage](#a6423190-acb7-11ea-9dd6-9b3aa12441f0)
        
        [2.1.2.5 LoadJob](#397c80e0-acaa-11ea-b509-dba22c0df611)
        
        [2.1.2.6 MetadataDocument](#f30a7c00-90d0-11eb-8e3d-f7c915bd922a)
        
        [2.1.2.7 Owner](#16ae7d10-acba-11ea-9dd6-9b3aa12441f0)
        
        [2.1.2.8 UserGroup](#2498f540-acba-11ea-9dd6-9b3aa12441f0)
        
*   [3. Relationships](#relationships)
    *   [3.1 fk DataFile.dataFileId to DataDocument.dataFileId](#9da63c60-d670-11ea-a396-6ba9749f6a74)
    *   [3.2 fk DataSource.dataSourceId to DataDocument.dataSourceId](#b814f5b0-dc05-11ea-96fb-8990ff2af80c)
    *   [3.3 fk DataSource.dataSourceId to DataFile.dataSourceId](#893cdd10-d670-11ea-a396-6ba9749f6a74)
    *   [3.4 fk Lineage.lineageId to LoadJob.lineageId](#d5f27110-acc2-11ea-99e3-9f008ba142d3)
    *   [3.5 fk LoadJob.loadJobId to DataFile.loadJobId](#9b671b40-acc2-11ea-99e3-9f008ba142d3)
    *   [3.6 fk Owner.ownerId to Lineage.ownerId](#cd2051b0-d670-11ea-a396-6ba9749f6a74)
    *   [3.7 fk UserGroup.userGroupId to Owner.userGroupId](#ca1c3970-d670-11ea-a396-6ba9749f6a74)

### <a id="model"></a>

##### 1\. Model

##### 1.1 Model **NWP and obs data**

##### 1.1.1 **NWP and obs data** Entity Relationship Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image2.png?raw=true)

##### 1.1.2 **NWP and obs data** Properties

##### 1.1.2.1 **Details** tab

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td><span>Model name</span></td><td>NWP and obs data</td></tr><tr><td><span>Technical name</span></td><td>NWP and obs data</td></tr><tr><td><span>Description</span></td><td><div class="docs-markdown"></div></td></tr><tr><td><span>Author</span></td><td></td></tr><tr><td><span>DB vendor</span></td><td>Couchbase</td></tr><tr><td><span>Version</span></td><td></td></tr><tr><td><span>Comments</span></td><td><div class="docs-markdown"></div></td></tr><tr><td><span>DB version</span></td><td>v6.0</td></tr><tr><td><span>Lineage</span></td><td></td></tr></tbody></table>

##### 1.1.3 **NWP and obs data** DB Definitions

### <a id="77c7f580-acb6-11ea-9dd6-9b3aa12441f0"></a>1.1.3.1 Field **type**

##### 1.1.3.1.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image3.png?raw=true)

##### 1.1.3.1.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td>DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="2e8e6250-acb6-11ea-9dd6-9b3aa12441f0"></a>1.1.3.2 Field **subset**

##### 1.1.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image4.png?raw=true)

##### 1.1.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr></tbody></table>

### <a id="containers"></a>

##### 2\. Buckets

### <a id="c1ca0f70-ac1f-11ea-b509-dba22c0df611"></a>2.1 Bucket **mdata**

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image5.png?raw=true)

##### 2.1.1 **mdata** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Bucket name</td><td>mdata</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket type</td><td>Couchbase</td></tr><tr><td colspan="2"><b>Memory size</b></td></tr><tr><td>Per node RAM quota</td><td>100</td></tr><tr><td>Cache Metadata</td><td>Value ejection</td></tr><tr><td>Access Control</td><td>Standart port</td></tr><tr><td>Password-protected</td><td></td></tr><tr><td>Conflict resolution</td><td>Sequence number</td></tr><tr><td colspan="2"><b>Replicas</b></td></tr><tr><td>Enable</td><td></td></tr><tr><td>Number</td><td>1</td></tr><tr><td>View index replicas</td><td></td></tr><tr><td colspan="2"><b>Disk I/O optimisation</b></td></tr><tr><td>Disk I/O priority</td><td>Low (Default)</td></tr><tr><td colspan="2"><b>Auto-Compaction</b></td></tr><tr><td>Override default</td><td></td></tr><tr><td colspan="2"><b>FLUSH</b></td></tr><tr><td>Enable</td><td></td></tr><tr><td colspan="2"><b>Key</b></td></tr><tr><td>Key</td><td>New Field</td></tr><tr><td>type</td><td>string</td></tr><tr><td>Document kind</td><td></td></tr><tr><td>type</td><td></td></tr></tbody></table>

### <a id="c1ca0f70-ac1f-11ea-b509-dba22c0df611-children"></a>2.1.2 **mdata** Document kinds

### <a id="df2f8770-acba-11ea-9dd6-9b3aa12441f0"></a>2.1.2.1 Document kind **DataDocument**

##### 2.1.2.1.1 **DataDocument** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image6.png?raw=true)

##### 2.1.2.1.2 **DataDocument** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>DataDocument</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.1.3 **DataDocument** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#1ec06e00-acbf-11ea-99e3-9f008ba142d3>dataDocumentId</a></td><td class="no-break-word">string</td><td>true</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#fcf834b0-acbe-11ea-99e3-9f008ba142d3>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#04f68fe0-acbf-11ea-99e3-9f008ba142d3>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#07eb93b0-d1b0-11ea-a396-6ba9749f6a74>docType</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>model, obs, CTC, sums, met</p></div></td></tr><tr><td><a href=#617c8060-d1b0-11ea-a396-6ba9749f6a74>dataFileId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#c511faa0-d283-11ea-a396-6ba9749f6a74>dataSourceId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#9b745d30-25cd-11eb-8f2d-bbdb6479fa56>version</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#486787c0-25cd-11eb-8f2d-bbdb6479fa56>model</a></td><td class="no-break-word">string</td><td>true</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#a853a6f0-25cd-11eb-8f2d-bbdb6479fa56>interpMethod</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#ed87cc30-d283-11ea-a396-6ba9749f6a74>geo</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Contains one or more lat lons</p></div></td></tr><tr><td><a href=#713a19d0-88ea-11eb-818f-2dec306dcc9c>region</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#fa8c5130-d283-11ea-a396-6ba9749f6a74>station</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#8f911790-d287-11ea-a396-6ba9749f6a74>leadTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#99294ca0-d287-11ea-a396-6ba9749f6a74>validTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#a946aa60-d287-11ea-a396-6ba9749f6a74>obsTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#b32907d0-d287-11ea-a396-6ba9749f6a74>ensMember</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#ba1d4600-d287-11ea-a396-6ba9749f6a74>data</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="1ec06e00-acbf-11ea-99e3-9f008ba142d3"></a>2.1.2.1.3.1 Field **dataDocumentId**

##### 2.1.2.1.3.1.1 **dataDocumentId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image7.png?raw=true)

##### 2.1.2.1.3.1.2 **dataDocumentId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>dataDocumentId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="fcf834b0-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.1.3.2 Field **subset**

##### 2.1.2.1.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image8.png?raw=true)

##### 2.1.2.1.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="04f68fe0-acbf-11ea-99e3-9f008ba142d3"></a>2.1.2.1.3.3 Field **type**

##### 2.1.2.1.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image9.png?raw=true)

##### 2.1.2.1.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="07eb93b0-d1b0-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.4 Field **docType**

##### 2.1.2.1.3.4.1 **docType** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image10.png?raw=true)

##### 2.1.2.1.3.4.2 **docType** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>docType</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>model, obs, CTC, sums, met</p></div></td></tr></tbody></table>

### <a id="617c8060-d1b0-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.5 Field **dataFileId**

##### 2.1.2.1.3.5.1 **dataFileId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image11.png?raw=true)

##### 2.1.2.1.3.5.2 **dataFileId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>dataFileId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td></tr><tr><td>Foreign field</td><td><a href=#1becd110-acbe-11ea-9dd6-9b3aa12441f0>dataFileId</a></td></tr><tr><td>Relationship type</td><td>Foreign Key</td></tr><tr><td>Relationship name</td><td>fk DataFile.dataFileId to DataDocument.dataFileId</td></tr><tr><td>Cardinality</td><td>1</td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c511faa0-d283-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.6 Field **dataSourceId**

##### 2.1.2.1.3.6.1 **dataSourceId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image12.png?raw=true)

##### 2.1.2.1.3.6.2 **dataSourceId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>dataSourceId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td><a href=#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0>DataSource</a></td></tr><tr><td>Foreign field</td><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td></tr><tr><td>Relationship type</td><td>Foreign Key</td></tr><tr><td>Relationship name</td><td>fk DataSource.dataSourceId to DataDocument.dataSourceId</td></tr><tr><td>Cardinality</td><td>1</td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="9b745d30-25cd-11eb-8f2d-bbdb6479fa56"></a>2.1.2.1.3.7 Field **version**

##### 2.1.2.1.3.7.1 **version** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image13.png?raw=true)

##### 2.1.2.1.3.7.2 **version** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>version</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="486787c0-25cd-11eb-8f2d-bbdb6479fa56"></a>2.1.2.1.3.8 Field **model**

##### 2.1.2.1.3.8.1 **model** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image14.png?raw=true)

##### 2.1.2.1.3.8.2 **model** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>model</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="a853a6f0-25cd-11eb-8f2d-bbdb6479fa56"></a>2.1.2.1.3.9 Field **interpMethod**

##### 2.1.2.1.3.9.1 **interpMethod** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image15.png?raw=true)

##### 2.1.2.1.3.9.2 **interpMethod** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>interpMethod</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="ed87cc30-d283-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.10 Field **geo**

##### 2.1.2.1.3.10.1 **geo** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image16.png?raw=true)

##### 2.1.2.1.3.10.2 **geo** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>geo</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Contains one or more lat lons</p></div></td></tr></tbody></table>

### <a id="713a19d0-88ea-11eb-818f-2dec306dcc9c"></a>2.1.2.1.3.11 Field **region**

##### 2.1.2.1.3.11.1 **region** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image17.png?raw=true)

##### 2.1.2.1.3.11.2 **region** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>region</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="fa8c5130-d283-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.12 Field **station**

##### 2.1.2.1.3.12.1 **station** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image18.png?raw=true)

##### 2.1.2.1.3.12.2 **station** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>station</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="8f911790-d287-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.13 Field **leadTime**

##### 2.1.2.1.3.13.1 **leadTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image19.png?raw=true)

##### 2.1.2.1.3.13.2 **leadTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>leadTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="99294ca0-d287-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.14 Field **validTime**

##### 2.1.2.1.3.14.1 **validTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image20.png?raw=true)

##### 2.1.2.1.3.14.2 **validTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>validTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="a946aa60-d287-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.15 Field **obsTime**

##### 2.1.2.1.3.15.1 **obsTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image21.png?raw=true)

##### 2.1.2.1.3.15.2 **obsTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>obsTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="b32907d0-d287-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.16 Field **ensMember**

##### 2.1.2.1.3.16.1 **ensMember** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image22.png?raw=true)

##### 2.1.2.1.3.16.2 **ensMember** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>ensMember</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="ba1d4600-d287-11ea-a396-6ba9749f6a74"></a>2.1.2.1.3.17 Field **data**

##### 2.1.2.1.3.17.1 **data** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image23.png?raw=true)

##### 2.1.2.1.3.17.2 **data** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>data</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.1.4 **DataDocument** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "DataDocument",
    "additionalProperties": false,
    "properties": {
        "dataDocumentId": {
            "type": "string"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "docType": {
            "type": "string"
        },
        "dataFileId": {
            "type": "string"
        },
        "dataSourceId": {
            "type": "string"
        },
        "version": {
            "type": "string"
        },
        "model": {
            "type": "string"
        },
        "interpMethod": {
            "type": "string"
        },
        "geo": {
            "type": "object",
            "additionalProperties": false
        },
        "region": {
            "type": "string"
        },
        "station": {
            "type": "string"
        },
        "leadTime": {
            "type": "string"
        },
        "validTime": {
            "type": "string"
        },
        "obsTime": {
            "type": "string"
        },
        "ensMember": {
            "type": "string"
        },
        "data": {
            "type": "object",
            "additionalProperties": false
        }
    },
    "required": [
        "dataDocumentId",
        "model"
    ]
}
```

##### 2.1.2.1.5 **DataDocument** JSON data

```
{
    "dataDocumentId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "docType": "Lorem",
    "dataFileId": "Lorem",
    "dataSourceId": "Lorem",
    "version": "Lorem",
    "model": "Lorem",
    "interpMethod": "Lorem",
    "geo": {},
    "region": "Lorem",
    "station": "Lorem",
    "leadTime": "Lorem",
    "validTime": "Lorem",
    "obsTime": "Lorem",
    "ensMember": "Lorem",
    "data": {}
}
```

##### 2.1.2.1.6 **DataDocument** Target Script

```
ottoman.model( "DataDocument",
{
    "dataDocumentId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "docType": {
        "type": "string"
    },
    "dataFileId": {
        "type": "string"
    },
    "dataSourceId": {
        "type": "string"
    },
    "version": {
        "type": "string"
    },
    "model": {
        "type": "string"
    },
    "interpMethod": {
        "type": "string"
    },
    "geo": {},
    "region": {
        "type": "string"
    },
    "station": {
        "type": "string"
    },
    "leadTime": {
        "type": "string"
    },
    "validTime": {
        "type": "string"
    },
    "obsTime": {
        "type": "string"
    },
    "ensMember": {
        "type": "string"
    },
    "data": {}
}
);
```

### <a id="49dd5370-acae-11ea-9dd6-9b3aa12441f0"></a>2.1.2.2 Document kind **DataFile**

##### 2.1.2.2.1 **DataFile** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image24.png?raw=true)

##### 2.1.2.2.2 **DataFile** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>DataFile</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.2.3 **DataFile** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#1becd110-acbe-11ea-9dd6-9b3aa12441f0>dataFileId</a></td><td class="no-break-word">string</td><td>true</td><td>dk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#bc902b30-acbe-11ea-99e3-9f008ba142d3>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#c22352c0-acbe-11ea-99e3-9f008ba142d3>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#21a13c40-acbe-11ea-9dd6-9b3aa12441f0>fileType</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>netCDF, grib2, stat, vsdb, mode</p></div></td></tr><tr><td><a href=#62407b00-acc1-11ea-99e3-9f008ba142d3>originType</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>MADIS, GSL, EMC, NSSL</p></div></td></tr><tr><td><a href=#6e164400-acc1-11ea-99e3-9f008ba142d3>loadJobId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#964632f0-acc1-11ea-99e3-9f008ba142d3>dataSourceId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#9f330cd0-acc1-11ea-99e3-9f008ba142d3>url</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Filename or web address that gives name and location of file</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#aa771d20-acc1-11ea-99e3-9f008ba142d3>projection</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#0ed04c10-d1b1-11ea-a396-6ba9749f6a74>interpolation</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="1becd110-acbe-11ea-9dd6-9b3aa12441f0"></a>2.1.2.2.3.1 Field **dataFileId**

##### 2.1.2.2.3.1.1 **dataFileId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image25.png?raw=true)

##### 2.1.2.2.3.1.2 **dataFileId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>dataFileId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="bc902b30-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.2 Field **subset**

##### 2.1.2.2.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image26.png?raw=true)

##### 2.1.2.2.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c22352c0-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.3 Field **type**

##### 2.1.2.2.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image27.png?raw=true)

##### 2.1.2.2.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="21a13c40-acbe-11ea-9dd6-9b3aa12441f0"></a>2.1.2.2.3.4 Field **fileType**

##### 2.1.2.2.3.4.1 **fileType** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image28.png?raw=true)

##### 2.1.2.2.3.4.2 **fileType** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fileType</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>netCDF, grib2, stat, vsdb, mode</p></div></td></tr></tbody></table>

### <a id="62407b00-acc1-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.5 Field **originType**

##### 2.1.2.2.3.5.1 **originType** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image29.png?raw=true)

##### 2.1.2.2.3.5.2 **originType** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>originType</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>MADIS, GSL, EMC, NSSL</p></div></td></tr></tbody></table>

### <a id="6e164400-acc1-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.6 Field **loadJobId**

##### 2.1.2.2.3.6.1 **loadJobId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image30.png?raw=true)

##### 2.1.2.2.3.6.2 **loadJobId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>loadJobId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td><a href=#397c80e0-acaa-11ea-b509-dba22c0df611>LoadJob</a></td></tr><tr><td>Foreign field</td><td><a href=#7464c3c0-acaa-11ea-b509-dba22c0df611>loadJobId</a></td></tr><tr><td>Relationship type</td><td>Foreign Key</td></tr><tr><td>Relationship name</td><td>fk LoadJob.loadJobId to DataFile.loadJobId</td></tr><tr><td>Cardinality</td><td>1</td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="964632f0-acc1-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.7 Field **dataSourceId**

##### 2.1.2.2.3.7.1 **dataSourceId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image31.png?raw=true)

##### 2.1.2.2.3.7.2 **dataSourceId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>dataSourceId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td><a href=#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0>DataSource</a></td></tr><tr><td>Foreign field</td><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td></tr><tr><td>Relationship type</td><td>Foreign Key</td></tr><tr><td>Relationship name</td><td>fk DataSource.dataSourceId to DataFile.dataSourceId</td></tr><tr><td>Cardinality</td><td>1</td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="9f330cd0-acc1-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.8 Field **url**

##### 2.1.2.2.3.8.1 **url** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image32.png?raw=true)

##### 2.1.2.2.3.8.2 **url** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>url</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>Filename or web address that gives name and location of file</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="aa771d20-acc1-11ea-99e3-9f008ba142d3"></a>2.1.2.2.3.9 Field **projection**

##### 2.1.2.2.3.9.1 **projection** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image33.png?raw=true)

##### 2.1.2.2.3.9.2 **projection** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>projection</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="0ed04c10-d1b1-11ea-a396-6ba9749f6a74"></a>2.1.2.2.3.10 Field **interpolation**

##### 2.1.2.2.3.10.1 **interpolation** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image34.png?raw=true)

##### 2.1.2.2.3.10.2 **interpolation** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>interpolation</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.2.4 **DataFile** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "DataFile",
    "additionalProperties": false,
    "properties": {
        "dataFileId": {
            "type": "string"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "fileType": {
            "type": "string"
        },
        "originType": {
            "type": "string"
        },
        "loadJobId": {
            "type": "string"
        },
        "dataSourceId": {
            "type": "string"
        },
        "url": {
            "type": "string",
            "description": "Filename or web address that gives name and location of file"
        },
        "projection": {
            "type": "string"
        },
        "interpolation": {
            "type": "string"
        }
    },
    "required": [
        "dataFileId"
    ]
}
```

##### 2.1.2.2.5 **DataFile** JSON data

```
{
    "dataFileId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "fileType": "Lorem",
    "originType": "Lorem",
    "loadJobId": "Lorem",
    "dataSourceId": "Lorem",
    "url": "Lorem",
    "projection": "Lorem",
    "interpolation": "Lorem"
}
```

##### 2.1.2.2.6 **DataFile** Target Script

```
ottoman.model( "DataFile",
{
    "dataFileId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "fileType": {
        "type": "string"
    },
    "originType": {
        "type": "string"
    },
    "loadJobId": {
        "type": "string"
    },
    "dataSourceId": {
        "type": "string"
    },
    "url": {
        "type": "string"
    },
    "projection": {
        "type": "string"
    },
    "interpolation": {
        "type": "string"
    }
}
);
```

### <a id="fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0"></a>2.1.2.3 Document kind **DataSource**

##### 2.1.2.3.1 **DataSource** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image35.png?raw=true)

##### 2.1.2.3.2 **DataSource** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>DataSource</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.3.3 **DataSource** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td><td class="no-break-word">string</td><td>true</td><td>dk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#a30aa910-acbe-11ea-99e3-9f008ba142d3>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#b0cefba0-acbe-11ea-99e3-9f008ba142d3>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#3e8d36c0-d1b1-11ea-a396-6ba9749f6a74>sourceType</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>METAR, RAOBS, HRRR</p></div></td></tr><tr><td><a href=#4690c210-d1b1-11ea-a396-6ba9749f6a74>geo</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#4b56d5a0-d1b1-11ea-a396-6ba9749f6a74>startTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#4f468660-d1b1-11ea-a396-6ba9749f6a74>endTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#55c641b0-d27e-11ea-a396-6ba9749f6a74>cadence</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#c182cc90-88f2-11eb-818f-2dec306dcc9c>cycleCadence</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>cycle seconds in MATS GUI metadata</p></div></td></tr><tr><td><a href=#5ed79380-d27e-11ea-a396-6ba9749f6a74>network</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#67627740-d27e-11ea-a396-6ba9749f6a74>current</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>True or false, if data source is still in use</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#704517f0-d27e-11ea-a396-6ba9749f6a74>validLeadTimes</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#7bcdab50-d27e-11ea-a396-6ba9749f6a74>variables</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d346fc80-88f2-11eb-818f-2dec306dcc9c>displayOrder</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#db4a12a0-88f2-11eb-818f-2dec306dcc9c>displayText</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#830d7170-d27e-11ea-a396-6ba9749f6a74>description</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.3.3.1 Field **dataSourceId**

##### 2.1.2.3.3.1.1 **dataSourceId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image36.png?raw=true)

##### 2.1.2.3.3.1.2 **dataSourceId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>dataSourceId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="a30aa910-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.3.3.2 Field **subset**

##### 2.1.2.3.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image37.png?raw=true)

##### 2.1.2.3.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="b0cefba0-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.3.3.3 Field **type**

##### 2.1.2.3.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image38.png?raw=true)

##### 2.1.2.3.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="3e8d36c0-d1b1-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.4 Field **sourceType**

##### 2.1.2.3.3.4.1 **sourceType** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image39.png?raw=true)

##### 2.1.2.3.3.4.2 **sourceType** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>sourceType</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>METAR, RAOBS, HRRR</p></div></td></tr></tbody></table>

### <a id="4690c210-d1b1-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.5 Field **geo**

##### 2.1.2.3.3.5.1 **geo** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image40.png?raw=true)

##### 2.1.2.3.3.5.2 **geo** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>geo</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="4b56d5a0-d1b1-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.6 Field **startTime**

##### 2.1.2.3.3.6.1 **startTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image41.png?raw=true)

##### 2.1.2.3.3.6.2 **startTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>startTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="4f468660-d1b1-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.7 Field **endTime**

##### 2.1.2.3.3.7.1 **endTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image42.png?raw=true)

##### 2.1.2.3.3.7.2 **endTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>endTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="55c641b0-d27e-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.8 Field **cadence**

##### 2.1.2.3.3.8.1 **cadence** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image43.png?raw=true)

##### 2.1.2.3.3.8.2 **cadence** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>cadence</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c182cc90-88f2-11eb-818f-2dec306dcc9c"></a>2.1.2.3.3.9 Field **cycleCadence**

##### 2.1.2.3.3.9.1 **cycleCadence** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image44.png?raw=true)

##### 2.1.2.3.3.9.2 **cycleCadence** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>cycleCadence</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>cycle seconds in MATS GUI metadata</p></div></td></tr></tbody></table>

### <a id="5ed79380-d27e-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.10 Field **network**

##### 2.1.2.3.3.10.1 **network** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image45.png?raw=true)

##### 2.1.2.3.3.10.2 **network** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>network</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="67627740-d27e-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.11 Field **current**

##### 2.1.2.3.3.11.1 **current** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image46.png?raw=true)

##### 2.1.2.3.3.11.2 **current** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>current</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>True or false, if data source is still in use</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="704517f0-d27e-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.12 Field **validLeadTimes**

##### 2.1.2.3.3.12.1 **validLeadTimes** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image47.png?raw=true)

##### 2.1.2.3.3.12.2 **validLeadTimes** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>validLeadTimes</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="7bcdab50-d27e-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.13 Field **variables**

##### 2.1.2.3.3.13.1 **variables** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image48.png?raw=true)

##### 2.1.2.3.3.13.2 **variables** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>variables</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d346fc80-88f2-11eb-818f-2dec306dcc9c"></a>2.1.2.3.3.14 Field **displayOrder**

##### 2.1.2.3.3.14.1 **displayOrder** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image49.png?raw=true)

##### 2.1.2.3.3.14.2 **displayOrder** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>displayOrder</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="db4a12a0-88f2-11eb-818f-2dec306dcc9c"></a>2.1.2.3.3.15 Field **displayText**

##### 2.1.2.3.3.15.1 **displayText** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image50.png?raw=true)

##### 2.1.2.3.3.15.2 **displayText** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>displayText</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="830d7170-d27e-11ea-a396-6ba9749f6a74"></a>2.1.2.3.3.16 Field **description**

##### 2.1.2.3.3.16.1 **description** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image51.png?raw=true)

##### 2.1.2.3.3.16.2 **description** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>description</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.3.4 **DataSource** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "DataSource",
    "additionalProperties": false,
    "properties": {
        "dataSourceId": {
            "type": "string"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "sourceType": {
            "type": "string"
        },
        "geo": {
            "type": "object",
            "additionalProperties": false
        },
        "startTime": {
            "type": "string"
        },
        "endTime": {
            "type": "string"
        },
        "cadence": {
            "type": "string"
        },
        "cycleCadence": {
            "type": "string"
        },
        "network": {
            "type": "string"
        },
        "current": {
            "type": "string",
            "description": "True or false, if data source is still in use"
        },
        "validLeadTimes": {
            "type": "string"
        },
        "variables": {
            "type": "string"
        },
        "displayOrder": {
            "type": "string"
        },
        "displayText": {
            "type": "string"
        },
        "description": {
            "type": "string"
        }
    },
    "required": [
        "dataSourceId"
    ]
}
```

##### 2.1.2.3.5 **DataSource** JSON data

```
{
    "dataSourceId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "sourceType": "Lorem",
    "geo": {},
    "startTime": "Lorem",
    "endTime": "Lorem",
    "cadence": "Lorem",
    "cycleCadence": "Lorem",
    "network": "Lorem",
    "current": "Lorem",
    "validLeadTimes": "Lorem",
    "variables": "Lorem",
    "displayOrder": "Lorem",
    "displayText": "Lorem",
    "description": "Lorem"
}
```

##### 2.1.2.3.6 **DataSource** Target Script

```
ottoman.model( "DataSource",
{
    "dataSourceId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "sourceType": {
        "type": "string"
    },
    "geo": {},
    "startTime": {
        "type": "string"
    },
    "endTime": {
        "type": "string"
    },
    "cadence": {
        "type": "string"
    },
    "cycleCadence": {
        "type": "string"
    },
    "network": {
        "type": "string"
    },
    "current": {
        "type": "string"
    },
    "validLeadTimes": {
        "type": "string"
    },
    "variables": {
        "type": "string"
    },
    "displayOrder": {
        "type": "string"
    },
    "displayText": {
        "type": "string"
    },
    "description": {
        "type": "string"
    }
}
);
```

### <a id="a6423190-acb7-11ea-9dd6-9b3aa12441f0"></a>2.1.2.4 Document kind **Lineage**

##### 2.1.2.4.1 **Lineage** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image52.png?raw=true)

##### 2.1.2.4.2 **Lineage** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>Lineage</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.4.3 **Lineage** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#d0edcee0-acb7-11ea-9dd6-9b3aa12441f0>lineageId</a></td><td class="no-break-word">string</td><td>true</td><td>dk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#c8940910-acbe-11ea-99e3-9f008ba142d3>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#19234640-acbd-11ea-9dd6-9b3aa12441f0>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#26b0d160-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#27561210-acbd-11ea-9dd6-9b3aa12441f0>creationTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#100f2df0-acc0-11ea-99e3-9f008ba142d3>updateTime</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#185ce1a0-acc0-11ea-99e3-9f008ba142d3>accessPolicy</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d0edcee0-acb7-11ea-9dd6-9b3aa12441f0"></a>2.1.2.4.3.1 Field **lineageId**

##### 2.1.2.4.3.1.1 **lineageId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image53.png?raw=true)

##### 2.1.2.4.3.1.2 **lineageId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>lineageId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c8940910-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.4.3.2 Field **subset**

##### 2.1.2.4.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image54.png?raw=true)

##### 2.1.2.4.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="19234640-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.4.3.3 Field **type**

##### 2.1.2.4.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image55.png?raw=true)

##### 2.1.2.4.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="26b0d160-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.4.3.4 Field **ownerId**

##### 2.1.2.4.3.4.1 **ownerId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image56.png?raw=true)

##### 2.1.2.4.3.4.2 **ownerId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>ownerId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td><a href=#16ae7d10-acba-11ea-9dd6-9b3aa12441f0>Owner</a></td></tr><tr><td>Foreign field</td><td><a href=#8995be30-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td></tr><tr><td>Relationship type</td><td>Foreign Key</td></tr><tr><td>Relationship name</td><td>fk Owner.ownerId to Lineage.ownerId</td></tr><tr><td>Cardinality</td><td>1</td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="27561210-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.4.3.5 Field **creationTime**

##### 2.1.2.4.3.5.1 **creationTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image57.png?raw=true)

##### 2.1.2.4.3.5.2 **creationTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>creationTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="100f2df0-acc0-11ea-99e3-9f008ba142d3"></a>2.1.2.4.3.6 Field **updateTime**

##### 2.1.2.4.3.6.1 **updateTime** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image58.png?raw=true)

##### 2.1.2.4.3.6.2 **updateTime** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>updateTime</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="185ce1a0-acc0-11ea-99e3-9f008ba142d3"></a>2.1.2.4.3.7 Field **accessPolicy**

##### 2.1.2.4.3.7.1 **accessPolicy** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image59.png?raw=true)

##### 2.1.2.4.3.7.2 **accessPolicy** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>accessPolicy</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.4.4 **Lineage** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "Lineage",
    "additionalProperties": false,
    "properties": {
        "lineageId": {
            "type": "string"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "ownerId": {
            "type": "string"
        },
        "creationTime": {
            "type": "string"
        },
        "updateTime": {
            "type": "string"
        },
        "accessPolicy": {
            "type": "string"
        }
    },
    "required": [
        "lineageId"
    ]
}
```

##### 2.1.2.4.5 **Lineage** JSON data

```
{
    "lineageId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "ownerId": "Lorem",
    "creationTime": "Lorem",
    "updateTime": "Lorem",
    "accessPolicy": "Lorem"
}
```

##### 2.1.2.4.6 **Lineage** Target Script

```
ottoman.model( "Lineage",
{
    "lineageId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "ownerId": {
        "type": "string"
    },
    "creationTime": {
        "type": "string"
    },
    "updateTime": {
        "type": "string"
    },
    "accessPolicy": {
        "type": "string"
    }
}
);
```

### <a id="397c80e0-acaa-11ea-b509-dba22c0df611"></a>2.1.2.5 Document kind **LoadJob**

##### 2.1.2.5.1 **LoadJob** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image60.png?raw=true)

##### 2.1.2.5.2 **LoadJob** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>LoadJob</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.5.3 **LoadJob** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#7464c3c0-acaa-11ea-b509-dba22c0df611>loadJobId</a></td><td class="no-break-word">string</td><td>true</td><td>dk</td><td><div class="docs-markdown"><p>Unique identifier for the load job</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#0db89b20-acbd-11ea-9dd6-9b3aa12441f0>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#e1eee3a0-acbc-11ea-9dd6-9b3aa12441f0>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#ccd81500-acb6-11ea-9dd6-9b3aa12441f0>lineageId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"><p>link to the lineage document for this LoadJob document</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#1aea8070-acb7-11ea-9dd6-9b3aa12441f0>script</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>program used to load data</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#295991f0-acb7-11ea-9dd6-9b3aa12441f0>scriptVersion</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>version of script used to load data</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#38e81d30-acb7-11ea-9dd6-9b3aa12441f0>loadSpec</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>config file used to load data. An XML loadspec for MET and vsdb data</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#6fe6f0e0-acb7-11ea-9dd6-9b3aa12441f0>note</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>user note describing data loaded</p></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="7464c3c0-acaa-11ea-b509-dba22c0df611"></a>2.1.2.5.3.1 Field **loadJobId**

##### 2.1.2.5.3.1.1 **loadJobId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image61.png?raw=true)

##### 2.1.2.5.3.1.2 **loadJobId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>loadJobId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>Unique identifier for the load job</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="0db89b20-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.2 Field **subset**

##### 2.1.2.5.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image62.png?raw=true)

##### 2.1.2.5.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="e1eee3a0-acbc-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.3 Field **type**

##### 2.1.2.5.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image63.png?raw=true)

##### 2.1.2.5.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="ccd81500-acb6-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.4 Field **lineageId**

##### 2.1.2.5.3.4.1 **lineageId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image64.png?raw=true)

##### 2.1.2.5.3.4.2 **lineageId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>lineageId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>link to the lineage document for this LoadJob document</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="1aea8070-acb7-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.5 Field **script**

##### 2.1.2.5.3.5.1 **script** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image65.png?raw=true)

##### 2.1.2.5.3.5.2 **script** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>script</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>program used to load data</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="295991f0-acb7-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.6 Field **scriptVersion**

##### 2.1.2.5.3.6.1 **scriptVersion** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image66.png?raw=true)

##### 2.1.2.5.3.6.2 **scriptVersion** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>scriptVersion</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>version of script used to load data</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="38e81d30-acb7-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.7 Field **loadSpec**

##### 2.1.2.5.3.7.1 **loadSpec** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image67.png?raw=true)

##### 2.1.2.5.3.7.2 **loadSpec** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>loadSpec</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>config file used to load data. An XML loadspec for MET and vsdb data</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="6fe6f0e0-acb7-11ea-9dd6-9b3aa12441f0"></a>2.1.2.5.3.8 Field **note**

##### 2.1.2.5.3.8.1 **note** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image68.png?raw=true)

##### 2.1.2.5.3.8.2 **note** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>note</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>user note describing data loaded</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.5.4 **LoadJob** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "LoadJob",
    "additionalProperties": false,
    "properties": {
        "loadJobId": {
            "type": "string",
            "description": "Unique identifier for the load job"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "lineageId": {
            "type": "string",
            "description": "link to the lineage document for this LoadJob document"
        },
        "script": {
            "type": "string",
            "description": "program used to load data"
        },
        "scriptVersion": {
            "type": "string",
            "description": "version of script used to load data"
        },
        "loadSpec": {
            "type": "string",
            "description": "config file used to load data. An XML loadspec for MET and vsdb data"
        },
        "note": {
            "type": "string",
            "description": "user note describing data loaded"
        }
    },
    "required": [
        "loadJobId"
    ]
}
```

##### 2.1.2.5.5 **LoadJob** JSON data

```
{
    "loadJobId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "lineageId": "Lorem",
    "script": "Lorem",
    "scriptVersion": "Lorem",
    "loadSpec": "Lorem",
    "note": "Lorem"
}
```

##### 2.1.2.5.6 **LoadJob** Target Script

```
ottoman.model( "LoadJob",
{
    "loadJobId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "lineageId": {
        "type": "string"
    },
    "script": {
        "type": "string"
    },
    "scriptVersion": {
        "type": "string"
    },
    "loadSpec": {
        "type": "string"
    },
    "note": {
        "type": "string"
    }
}
);
```

### <a id="f30a7c00-90d0-11eb-8e3d-f7c915bd922a"></a>2.1.2.6 Document kind **MetadataDocument**

##### 2.1.2.6.1 **MetadataDocument** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image69.png?raw=true)

##### 2.1.2.6.2 **MetadataDocument** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>MetadataDocument</td></tr><tr><td>Technical name</td><td>MD</td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3 **MetadataDocument** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#457049c0-90d6-11eb-8e3d-f7c915bd922a>MetadataDocumentId</a></td><td class="no-break-word">string</td><td>true</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>matsGui, matsAux</p></div></td></tr><tr><td><a href=#d69bee40-90d1-11eb-8e3d-f7c915bd922a>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, COMMON, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#74999940-90d1-11eb-8e3d-f7c915bd922a>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#c890ceb0-90d1-11eb-8e3d-f7c915bd922a>docType</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>matsGui, matsAux, region</p></div></td></tr><tr><td><a href=#f884cef0-90d1-11eb-8e3d-f7c915bd922a>version</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#04acfdb0-90d2-11eb-8e3d-f7c915bd922a>model</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#09290ff0-90d2-11eb-8e3d-f7c915bd922a>app</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#cfd40550-90d3-11eb-8e3d-f7c915bd922a>displayText</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#fb886010-90d3-11eb-8e3d-f7c915bd922a>displayOrder</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#08494c60-90d4-11eb-8e3d-f7c915bd922a>displayCategory</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#87c03800-90d4-11eb-8e3d-f7c915bd922a>mindate</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#3c6ee860-90d4-11eb-8e3d-f7c915bd922a>maxdate</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#9726c8e0-90d4-11eb-8e3d-f7c915bd922a>numrecs</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d26ce2f0-9173-11eb-8e3d-f7c915bd922a>name</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>name of region for region metadata</p></div></td></tr><tr><td><a href=#ed596f20-9173-11eb-8e3d-f7c915bd922a>description</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#f98d84c0-9173-11eb-8e3d-f7c915bd922a>geo</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#9c8d1410-90d4-11eb-8e3d-f7c915bd922a>updated</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#f841cb20-90d4-11eb-8e3d-f7c915bd922a>regions</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#fdfdd770-90d4-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#01547c80-90d5-11eb-8e3d-f7c915bd922a>thresholds</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#0d9bf310-90d5-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#158a21a0-90d5-11eb-8e3d-f7c915bd922a>fcstLens</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#1f29a9b0-90d5-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#f534aad0-90d7-11eb-8e3d-f7c915bd922a>fcstTypes</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#fa883150-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#75bb0010-90d7-11eb-8e3d-f7c915bd922a>scales</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#8726afc0-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#8d4e9d40-90d7-11eb-8e3d-f7c915bd922a>levels</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#a89a8780-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#bceb2eb0-90d7-11eb-8e3d-f7c915bd922a>sources</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#c388e870-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#c6cacd00-90d7-11eb-8e3d-f7c915bd922a>variables</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d46a3270-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#0950d480-90d8-11eb-8e3d-f7c915bd922a>vgtyps</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#0ebe99c0-90d8-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d0fce2c0-9174-11eb-8e3d-f7c915bd922a>standardizedModelList</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that has a list of pairs with internal name and display name for models</p></div></td></tr><tr><td><a href=#fb184e00-9174-11eb-8e3d-f7c915bd922a>primaryModelOrders</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each model has an mOrder and possibly a cycleSeconds</p></div></td></tr><tr><td><a href=#77c0ee70-9225-11eb-8e3d-f7c915bd922a>modelName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#896d0280-9225-11eb-8e3d-f7c915bd922a>mOrder</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#b1fdaf10-9225-11eb-8e3d-f7c915bd922a>cycleSeconds</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#be167480-9225-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#054967b0-9175-11eb-8e3d-f7c915bd922a>thresholdDescriptions</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr><tr><td><a href=#147d4a60-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#1f7efa80-9226-11eb-8e3d-f7c915bd922a>^[threshold]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#20a02760-9175-11eb-8e3d-f7c915bd922a>fcstTypeDescriptions</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr><tr><td><a href=#3ec4a7a0-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#45783350-9226-11eb-8e3d-f7c915bd922a>^[fcstType]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#43ff7b70-9175-11eb-8e3d-f7c915bd922a>scaleDescriptions</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr><tr><td><a href=#636d6ba0-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#6e460ff0-9226-11eb-8e3d-f7c915bd922a>^[scale]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#2c6a7eb0-9175-11eb-8e3d-f7c915bd922a>vgtypDescriptions</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr><tr><td><a href=#b68cdeb0-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#bdc72690-9226-11eb-8e3d-f7c915bd922a>^[vgtyp]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#578fdfe0-9175-11eb-8e3d-f7c915bd922a>ptypeDescriptions</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr><tr><td><a href=#cc053d00-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d3e67340-9226-11eb-8e3d-f7c915bd922a>^[ptype]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#6ef20cd0-9175-11eb-8e3d-f7c915bd922a>stationDescriptions</a></td><td class="no-break-word">object</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr><tr><td><a href=#df843c50-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#f3a39a50-9226-11eb-8e3d-f7c915bd922a>^[station]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="457049c0-90d6-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.1 Field **MetadataDocumentId**

##### 2.1.2.6.3.1.1 **MetadataDocumentId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image70.png?raw=true)

##### 2.1.2.6.3.1.2 **MetadataDocumentId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>MetadataDocumentId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>matsGui, matsAux</p></div></td></tr></tbody></table>

### <a id="d69bee40-90d1-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.2 Field **subset**

##### 2.1.2.6.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image71.png?raw=true)

##### 2.1.2.6.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>METAR, COMMON, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr></tbody></table>

### <a id="74999940-90d1-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.3 Field **type**

##### 2.1.2.6.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image72.png?raw=true)

##### 2.1.2.6.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td>DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c890ceb0-90d1-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.4 Field **docType**

##### 2.1.2.6.3.4.1 **docType** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image73.png?raw=true)

##### 2.1.2.6.3.4.2 **docType** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>docType</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>matsGui, matsAux, region</p></div></td></tr></tbody></table>

### <a id="f884cef0-90d1-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.5 Field **version**

##### 2.1.2.6.3.5.1 **version** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image74.png?raw=true)

##### 2.1.2.6.3.5.2 **version** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>version</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="04acfdb0-90d2-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.6 Field **model**

##### 2.1.2.6.3.6.1 **model** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image75.png?raw=true)

##### 2.1.2.6.3.6.2 **model** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>model</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="09290ff0-90d2-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.7 Field **app**

##### 2.1.2.6.3.7.1 **app** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image76.png?raw=true)

##### 2.1.2.6.3.7.2 **app** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>app</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="cfd40550-90d3-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.8 Field **displayText**

##### 2.1.2.6.3.8.1 **displayText** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image77.png?raw=true)

##### 2.1.2.6.3.8.2 **displayText** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>displayText</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="fb886010-90d3-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.9 Field **displayOrder**

##### 2.1.2.6.3.9.1 **displayOrder** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image78.png?raw=true)

##### 2.1.2.6.3.9.2 **displayOrder** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>displayOrder</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>number</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Unit</td><td></td></tr><tr><td>Min value</td><td></td></tr><tr><td>Excl min</td><td></td></tr><tr><td>Max value</td><td></td></tr><tr><td>Excl max</td><td></td></tr><tr><td>Multiple of</td><td></td></tr><tr><td>Divisible by</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="08494c60-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.10 Field **displayCategory**

##### 2.1.2.6.3.10.1 **displayCategory** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image79.png?raw=true)

##### 2.1.2.6.3.10.2 **displayCategory** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>displayCategory</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>number</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Unit</td><td></td></tr><tr><td>Min value</td><td></td></tr><tr><td>Excl min</td><td></td></tr><tr><td>Max value</td><td></td></tr><tr><td>Excl max</td><td></td></tr><tr><td>Multiple of</td><td></td></tr><tr><td>Divisible by</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="87c03800-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.11 Field **mindate**

##### 2.1.2.6.3.11.1 **mindate** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image80.png?raw=true)

##### 2.1.2.6.3.11.2 **mindate** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>mindate</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>number</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Unit</td><td></td></tr><tr><td>Min value</td><td></td></tr><tr><td>Excl min</td><td></td></tr><tr><td>Max value</td><td></td></tr><tr><td>Excl max</td><td></td></tr><tr><td>Multiple of</td><td></td></tr><tr><td>Divisible by</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="3c6ee860-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.12 Field **maxdate**

##### 2.1.2.6.3.12.1 **maxdate** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image81.png?raw=true)

##### 2.1.2.6.3.12.2 **maxdate** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>maxdate</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>number</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Unit</td><td></td></tr><tr><td>Min value</td><td></td></tr><tr><td>Excl min</td><td></td></tr><tr><td>Max value</td><td></td></tr><tr><td>Excl max</td><td></td></tr><tr><td>Multiple of</td><td></td></tr><tr><td>Divisible by</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="9726c8e0-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.13 Field **numrecs**

##### 2.1.2.6.3.13.1 **numrecs** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image82.png?raw=true)

##### 2.1.2.6.3.13.2 **numrecs** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>numrecs</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>number</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Unit</td><td></td></tr><tr><td>Min value</td><td></td></tr><tr><td>Excl min</td><td></td></tr><tr><td>Max value</td><td></td></tr><tr><td>Excl max</td><td></td></tr><tr><td>Multiple of</td><td></td></tr><tr><td>Divisible by</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d26ce2f0-9173-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.14 Field **name**

##### 2.1.2.6.3.14.1 **name** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image83.png?raw=true)

##### 2.1.2.6.3.14.2 **name** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>name</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>name of region for region metadata</p></div></td></tr></tbody></table>

### <a id="ed596f20-9173-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.15 Field **description**

##### 2.1.2.6.3.15.1 **description** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image84.png?raw=true)

##### 2.1.2.6.3.15.2 **description** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>description</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="f98d84c0-9173-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.16 Field **geo**

##### 2.1.2.6.3.16.1 **geo** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image85.png?raw=true)

##### 2.1.2.6.3.16.2 **geo** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>geo</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="9c8d1410-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.17 Field **updated**

##### 2.1.2.6.3.17.1 **updated** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image86.png?raw=true)

##### 2.1.2.6.3.17.2 **updated** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>updated</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="f841cb20-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.18 Field **regions**

##### 2.1.2.6.3.18.1 **regions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image87.png?raw=true)

##### 2.1.2.6.3.18.2 **regions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#fdfdd770-90d4-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.18.3 **regions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>regions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="fdfdd770-90d4-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.19 Field **\[0\]**

##### 2.1.2.6.3.19.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image88.png?raw=true)

##### 2.1.2.6.3.19.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="01547c80-90d5-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.20 Field **thresholds**

##### 2.1.2.6.3.20.1 **thresholds** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image89.png?raw=true)

##### 2.1.2.6.3.20.2 **thresholds** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#0d9bf310-90d5-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.20.3 **thresholds** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>thresholds</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="0d9bf310-90d5-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.21 Field **\[0\]**

##### 2.1.2.6.3.21.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image90.png?raw=true)

##### 2.1.2.6.3.21.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="158a21a0-90d5-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.22 Field **fcstLens**

##### 2.1.2.6.3.22.1 **fcstLens** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image91.png?raw=true)

##### 2.1.2.6.3.22.2 **fcstLens** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#1f29a9b0-90d5-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.22.3 **fcstLens** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fcstLens</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="1f29a9b0-90d5-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.23 Field **\[0\]**

##### 2.1.2.6.3.23.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image92.png?raw=true)

##### 2.1.2.6.3.23.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="f534aad0-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.24 Field **fcstTypes**

##### 2.1.2.6.3.24.1 **fcstTypes** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image93.png?raw=true)

##### 2.1.2.6.3.24.2 **fcstTypes** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#fa883150-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.24.3 **fcstTypes** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fcstTypes</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="fa883150-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.25 Field **\[0\]**

##### 2.1.2.6.3.25.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image94.png?raw=true)

##### 2.1.2.6.3.25.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="75bb0010-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.26 Field **scales**

##### 2.1.2.6.3.26.1 **scales** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image95.png?raw=true)

##### 2.1.2.6.3.26.2 **scales** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#8726afc0-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.26.3 **scales** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>scales</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="8726afc0-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.27 Field **\[0\]**

##### 2.1.2.6.3.27.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image96.png?raw=true)

##### 2.1.2.6.3.27.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="8d4e9d40-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.28 Field **levels**

##### 2.1.2.6.3.28.1 **levels** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image97.png?raw=true)

##### 2.1.2.6.3.28.2 **levels** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#a89a8780-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.28.3 **levels** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>levels</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="a89a8780-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.29 Field **\[0\]**

##### 2.1.2.6.3.29.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image98.png?raw=true)

##### 2.1.2.6.3.29.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="bceb2eb0-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.30 Field **sources**

##### 2.1.2.6.3.30.1 **sources** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image99.png?raw=true)

##### 2.1.2.6.3.30.2 **sources** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#c388e870-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.30.3 **sources** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>sources</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c388e870-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.31 Field **\[0\]**

##### 2.1.2.6.3.31.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image100.png?raw=true)

##### 2.1.2.6.3.31.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="c6cacd00-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.32 Field **variables**

##### 2.1.2.6.3.32.1 **variables** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image101.png?raw=true)

##### 2.1.2.6.3.32.2 **variables** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#d46a3270-90d7-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.32.3 **variables** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>variables</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d46a3270-90d7-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.33 Field **\[0\]**

##### 2.1.2.6.3.33.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image102.png?raw=true)

##### 2.1.2.6.3.33.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="0950d480-90d8-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.34 Field **vgtyps**

##### 2.1.2.6.3.34.1 **vgtyps** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image103.png?raw=true)

##### 2.1.2.6.3.34.2 **vgtyps** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#0ebe99c0-90d8-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.34.3 **vgtyps** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>vgtyps</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="0ebe99c0-90d8-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.35 Field **\[0\]**

##### 2.1.2.6.3.35.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image104.png?raw=true)

##### 2.1.2.6.3.35.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d0fce2c0-9174-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.36 Field **standardizedModelList**

##### 2.1.2.6.3.36.1 **standardizedModelList** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image105.png?raw=true)

##### 2.1.2.6.3.36.2 **standardizedModelList** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>standardizedModelList</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that has a list of pairs with internal name and display name for models</p></div></td></tr></tbody></table>

### <a id="fb184e00-9174-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.37 Field **primaryModelOrders**

##### 2.1.2.6.3.37.1 **primaryModelOrders** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image106.png?raw=true)

##### 2.1.2.6.3.37.2 **primaryModelOrders** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#77c0ee70-9225-11eb-8e3d-f7c915bd922a>modelName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#896d0280-9225-11eb-8e3d-f7c915bd922a>mOrder</a></td><td class="no-break-word">number</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#b1fdaf10-9225-11eb-8e3d-f7c915bd922a>cycleSeconds</a></td><td class="no-break-word">array</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.37.3 **primaryModelOrders** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>primaryModelOrders</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each model has an mOrder and possibly a cycleSeconds</p></div></td></tr></tbody></table>

### <a id="77c0ee70-9225-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.38 Field **modelName+**

##### 2.1.2.6.3.38.1 **modelName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image107.png?raw=true)

##### 2.1.2.6.3.38.2 **modelName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>modelName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="896d0280-9225-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.39 Field **mOrder**

##### 2.1.2.6.3.39.1 **mOrder** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image108.png?raw=true)

##### 2.1.2.6.3.39.2 **mOrder** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>mOrder</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>number</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Unit</td><td></td></tr><tr><td>Min value</td><td></td></tr><tr><td>Excl min</td><td></td></tr><tr><td>Max value</td><td></td></tr><tr><td>Excl max</td><td></td></tr><tr><td>Multiple of</td><td></td></tr><tr><td>Divisible by</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="b1fdaf10-9225-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.40 Field **cycleSeconds**

##### 2.1.2.6.3.40.1 **cycleSeconds** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image109.png?raw=true)

##### 2.1.2.6.3.40.2 **cycleSeconds** Hierarchy

Parent field: **primaryModelOrders**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#be167480-9225-11eb-8e3d-f7c915bd922a>[0]</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.40.3 **cycleSeconds** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>cycleSeconds</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>array</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min items</td><td></td></tr><tr><td>Max items</td><td></td></tr><tr><td>Unique items</td><td></td></tr><tr><td>Additional items</td><td>true</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="be167480-9225-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.41 Field **\[0\]**

##### 2.1.2.6.3.41.1 **\[0\]** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image110.png?raw=true)

##### 2.1.2.6.3.41.2 **\[0\]** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Display name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="054967b0-9175-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.42 Field **thresholdDescriptions**

##### 2.1.2.6.3.42.1 **thresholdDescriptions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image111.png?raw=true)

##### 2.1.2.6.3.42.2 **thresholdDescriptions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#147d4a60-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#1f7efa80-9226-11eb-8e3d-f7c915bd922a>^[threshold]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.42.3 **thresholdDescriptions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>thresholdDescriptions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr></tbody></table>

### <a id="147d4a60-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.43 Field **appName+**

##### 2.1.2.6.3.43.1 **appName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image112.png?raw=true)

##### 2.1.2.6.3.43.2 **appName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>appName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="1f7efa80-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.44 Field **^\[threshold\]+$**

##### 2.1.2.6.3.44.1 **^\[threshold\]+$** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image113.png?raw=true)

##### 2.1.2.6.3.44.2 **^\[threshold\]+$** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>^[threshold]+$</td></tr><tr><td>Sample Name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="20a02760-9175-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.45 Field **fcstTypeDescriptions**

##### 2.1.2.6.3.45.1 **fcstTypeDescriptions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image114.png?raw=true)

##### 2.1.2.6.3.45.2 **fcstTypeDescriptions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#3ec4a7a0-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#45783350-9226-11eb-8e3d-f7c915bd922a>^[fcstType]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.45.3 **fcstTypeDescriptions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fcstTypeDescriptions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr></tbody></table>

### <a id="3ec4a7a0-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.46 Field **appName+**

##### 2.1.2.6.3.46.1 **appName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image115.png?raw=true)

##### 2.1.2.6.3.46.2 **appName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>appName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="45783350-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.47 Field **^\[fcstType\]+$**

##### 2.1.2.6.3.47.1 **^\[fcstType\]+$** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image116.png?raw=true)

##### 2.1.2.6.3.47.2 **^\[fcstType\]+$** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>^[fcstType]+$</td></tr><tr><td>Sample Name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="43ff7b70-9175-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.48 Field **scaleDescriptions**

##### 2.1.2.6.3.48.1 **scaleDescriptions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image117.png?raw=true)

##### 2.1.2.6.3.48.2 **scaleDescriptions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#636d6ba0-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#6e460ff0-9226-11eb-8e3d-f7c915bd922a>^[scale]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.48.3 **scaleDescriptions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>scaleDescriptions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr></tbody></table>

### <a id="636d6ba0-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.49 Field **appName+**

##### 2.1.2.6.3.49.1 **appName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image118.png?raw=true)

##### 2.1.2.6.3.49.2 **appName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>appName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="6e460ff0-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.50 Field **^\[scale\]+$**

##### 2.1.2.6.3.50.1 **^\[scale\]+$** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image119.png?raw=true)

##### 2.1.2.6.3.50.2 **^\[scale\]+$** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>^[scale]+$</td></tr><tr><td>Sample Name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="2c6a7eb0-9175-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.51 Field **vgtypDescriptions**

##### 2.1.2.6.3.51.1 **vgtypDescriptions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image120.png?raw=true)

##### 2.1.2.6.3.51.2 **vgtypDescriptions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#b68cdeb0-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#bdc72690-9226-11eb-8e3d-f7c915bd922a>^[vgtyp]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.51.3 **vgtypDescriptions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>vgtypDescriptions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr></tbody></table>

### <a id="b68cdeb0-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.52 Field **appName+**

##### 2.1.2.6.3.52.1 **appName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image121.png?raw=true)

##### 2.1.2.6.3.52.2 **appName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>appName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="bdc72690-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.53 Field **^\[vgtyp\]+$**

##### 2.1.2.6.3.53.1 **^\[vgtyp\]+$** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image122.png?raw=true)

##### 2.1.2.6.3.53.2 **^\[vgtyp\]+$** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>^[vgtyp]+$</td></tr><tr><td>Sample Name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="578fdfe0-9175-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.54 Field **ptypeDescriptions**

##### 2.1.2.6.3.54.1 **ptypeDescriptions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image123.png?raw=true)

##### 2.1.2.6.3.54.2 **ptypeDescriptions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#cc053d00-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d3e67340-9226-11eb-8e3d-f7c915bd922a>^[ptype]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.54.3 **ptypeDescriptions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>ptypeDescriptions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr></tbody></table>

### <a id="cc053d00-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.55 Field **appName+**

##### 2.1.2.6.3.55.1 **appName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image124.png?raw=true)

##### 2.1.2.6.3.55.2 **appName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>appName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d3e67340-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.56 Field **^\[ptype\]+$**

##### 2.1.2.6.3.56.1 **^\[ptype\]+$** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image125.png?raw=true)

##### 2.1.2.6.3.56.2 **^\[ptype\]+$** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>^[ptype]+$</td></tr><tr><td>Sample Name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="6ef20cd0-9175-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.57 Field **stationDescriptions**

##### 2.1.2.6.3.57.1 **stationDescriptions** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image126.png?raw=true)

##### 2.1.2.6.3.57.2 **stationDescriptions** Hierarchy

Parent field: **MetadataDocument**

Child field(s):

<table class="field-properties-table"><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#df843c50-9226-11eb-8e3d-f7c915bd922a>appName+</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#f3a39a50-9226-11eb-8e3d-f7c915bd922a>^[station]+$</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.3.57.3 **stationDescriptions** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>stationDescriptions</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>object</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Min Properties</td><td></td></tr><tr><td>Max Properties</td><td></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"><p>Object that for each app has a list of pairs with internal name and display name</p></div></td></tr></tbody></table>

### <a id="df843c50-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.58 Field **appName+**

##### 2.1.2.6.3.58.1 **appName+** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image127.png?raw=true)

##### 2.1.2.6.3.58.2 **appName+** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>appName+</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="f3a39a50-9226-11eb-8e3d-f7c915bd922a"></a>2.1.2.6.3.59 Field **^\[station\]+$**

##### 2.1.2.6.3.59.1 **^\[station\]+$** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image128.png?raw=true)

##### 2.1.2.6.3.59.2 **^\[station\]+$** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>^[station]+$</td></tr><tr><td>Sample Name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.6.4 **MetadataDocument** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "MetadataDocument",
    "additionalProperties": false,
    "properties": {
        "MetadataDocumentId": {
            "type": "string"
        },
        "subset": {
            "type": "string",
            "description": "Similar to \"database\" in MySQL - a subset or subdivision of the data"
        },
        "type": {
            "type": "string",
            "description": "The type of the document - similar to a table in a relational database"
        },
        "docType": {
            "type": "string"
        },
        "version": {
            "type": "string"
        },
        "model": {
            "type": "string"
        },
        "app": {
            "type": "string"
        },
        "displayText": {
            "type": "string"
        },
        "displayOrder": {
            "type": "number"
        },
        "displayCategory": {
            "type": "number"
        },
        "mindate": {
            "type": "number"
        },
        "maxdate": {
            "type": "number"
        },
        "numrecs": {
            "type": "number"
        },
        "name": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "geo": {
            "type": "object",
            "additionalProperties": false
        },
        "updated": {
            "type": "string"
        },
        "regions": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "thresholds": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "fcstLens": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "fcstTypes": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "scales": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "levels": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "sources": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "variables": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "vgtyps": {
            "type": "array",
            "additionalItems": true,
            "items": {
                "type": "string"
            }
        },
        "standardizedModelList": {
            "type": "object",
            "additionalProperties": false
        },
        "primaryModelOrders": {
            "type": "object",
            "properties": {
                "modelName+": {
                    "type": "string"
                },
                "mOrder": {
                    "type": "number"
                },
                "cycleSeconds": {
                    "type": "array",
                    "additionalItems": true,
                    "items": {
                        "type": "string"
                    }
                }
            },
            "additionalProperties": false
        },
        "thresholdDescriptions": {
            "type": "object",
            "properties": {
                "appName+": {
                    "type": "string"
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "^[threshold]+$": {
                    "type": "string"
                }
            }
        },
        "fcstTypeDescriptions": {
            "type": "object",
            "properties": {
                "appName+": {
                    "type": "string"
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "^[fcstType]+$": {
                    "type": "string"
                }
            }
        },
        "scaleDescriptions": {
            "type": "object",
            "properties": {
                "appName+": {
                    "type": "string"
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "^[scale]+$": {
                    "type": "string"
                }
            }
        },
        "vgtypDescriptions": {
            "type": "object",
            "properties": {
                "appName+": {
                    "type": "string"
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "^[vgtyp]+$": {
                    "type": "string"
                }
            }
        },
        "ptypeDescriptions": {
            "type": "object",
            "properties": {
                "appName+": {
                    "type": "string"
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "^[ptype]+$": {
                    "type": "string"
                }
            }
        },
        "stationDescriptions": {
            "type": "object",
            "properties": {
                "appName+": {
                    "type": "string"
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "^[station]+$": {
                    "type": "string"
                }
            }
        }
    },
    "required": [
        "MetadataDocumentId"
    ]
}
```

##### 2.1.2.6.5 **MetadataDocument** JSON data

```
{
    "MetadataDocumentId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "docType": "Lorem",
    "version": "Lorem",
    "model": "Lorem",
    "app": "Lorem",
    "displayText": "Lorem",
    "displayOrder": -14,
    "displayCategory": -42,
    "mindate": -39,
    "maxdate": -74,
    "numrecs": -71,
    "name": "Lorem",
    "description": "Lorem",
    "geo": {},
    "updated": "Lorem",
    "regions": [
        "Lorem"
    ],
    "thresholds": [
        "Lorem"
    ],
    "fcstLens": [
        "Lorem"
    ],
    "fcstTypes": [
        "Lorem"
    ],
    "scales": [
        "Lorem"
    ],
    "levels": [
        "Lorem"
    ],
    "sources": [
        "Lorem"
    ],
    "variables": [
        "Lorem"
    ],
    "vgtyps": [
        "Lorem"
    ],
    "standardizedModelList": {},
    "primaryModelOrders": {
        "modelName+": "Lorem",
        "mOrder": 85,
        "cycleSeconds": [
            "Lorem"
        ]
    },
    "thresholdDescriptions": {
        "appName+": "Lorem"
    },
    "fcstTypeDescriptions": {
        "appName+": "Lorem"
    },
    "scaleDescriptions": {
        "appName+": "Lorem"
    },
    "vgtypDescriptions": {
        "appName+": "Lorem"
    },
    "ptypeDescriptions": {
        "appName+": "Lorem"
    },
    "stationDescriptions": {
        "appName+": "Lorem"
    }
}
```

##### 2.1.2.6.6 **MetadataDocument** Target Script

```
ottoman.model( "MD",
{
    "MetadataDocumentId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "docType": {
        "type": "string"
    },
    "version": {
        "type": "string"
    },
    "model": {
        "type": "string"
    },
    "app": {
        "type": "string"
    },
    "displayText": {
        "type": "string"
    },
    "displayOrder": {
        "type": "number"
    },
    "displayCategory": {
        "type": "number"
    },
    "mindate": {
        "type": "number"
    },
    "maxdate": {
        "type": "number"
    },
    "numrecs": {
        "type": "number"
    },
    "name": {
        "type": "string"
    },
    "description": {
        "type": "string"
    },
    "geo": {},
    "updated": {
        "type": "string"
    },
    "regions": [
        "string"
    ],
    "thresholds": [
        "string"
    ],
    "fcstLens": [
        "string"
    ],
    "fcstTypes": [
        "string"
    ],
    "scales": [
        "string"
    ],
    "levels": [
        "string"
    ],
    "sources": [
        "string"
    ],
    "variables": [
        "string"
    ],
    "vgtyps": [
        "string"
    ],
    "standardizedModelList": {},
    "primaryModelOrders": {
        "modelName+": {
            "type": "string"
        },
        "mOrder": {
            "type": "number"
        },
        "cycleSeconds": [
            "string"
        ]
    },
    "thresholdDescriptions": {
        "appName+": {
            "type": "string"
        },
        "^[threshold]+$": {
            "type": "string"
        }
    },
    "fcstTypeDescriptions": {
        "appName+": {
            "type": "string"
        },
        "^[fcstType]+$": {
            "type": "string"
        }
    },
    "scaleDescriptions": {
        "appName+": {
            "type": "string"
        },
        "^[scale]+$": {
            "type": "string"
        }
    },
    "vgtypDescriptions": {
        "appName+": {
            "type": "string"
        },
        "^[vgtyp]+$": {
            "type": "string"
        }
    },
    "ptypeDescriptions": {
        "appName+": {
            "type": "string"
        },
        "^[ptype]+$": {
            "type": "string"
        }
    },
    "stationDescriptions": {
        "appName+": {
            "type": "string"
        },
        "^[station]+$": {
            "type": "string"
        }
    }
}
);
```

### <a id="16ae7d10-acba-11ea-9dd6-9b3aa12441f0"></a>2.1.2.7 Document kind **Owner**

##### 2.1.2.7.1 **Owner** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image129.png?raw=true)

##### 2.1.2.7.2 **Owner** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>Owner</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.7.3 **Owner** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#8995be30-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td><td class="no-break-word">string</td><td>true</td><td>dk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#85282ae0-acbd-11ea-9dd6-9b3aa12441f0>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#d0268310-acbe-11ea-99e3-9f008ba142d3>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#69d9a130-acc0-11ea-99e3-9f008ba142d3>userGroupId</a></td><td class="no-break-word">string</td><td>false</td><td>fk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#93637ad0-acc0-11ea-99e3-9f008ba142d3>contact</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>User contact info, probably email address</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#9e3e1af0-acc0-11ea-99e3-9f008ba142d3>description</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="8995be30-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.7.3.1 Field **ownerId**

##### 2.1.2.7.3.1.1 **ownerId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image130.png?raw=true)

##### 2.1.2.7.3.1.2 **ownerId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>ownerId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="85282ae0-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.7.3.2 Field **subset**

##### 2.1.2.7.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image131.png?raw=true)

##### 2.1.2.7.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d0268310-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.7.3.3 Field **type**

##### 2.1.2.7.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image132.png?raw=true)

##### 2.1.2.7.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="69d9a130-acc0-11ea-99e3-9f008ba142d3"></a>2.1.2.7.3.4 Field **userGroupId**

##### 2.1.2.7.3.4.1 **userGroupId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image133.png?raw=true)

##### 2.1.2.7.3.4.2 **userGroupId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>userGroupId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td><a href=#2498f540-acba-11ea-9dd6-9b3aa12441f0>UserGroup</a></td></tr><tr><td>Foreign field</td><td><a href=#7062eff0-acbd-11ea-9dd6-9b3aa12441f0>userGroupId</a></td></tr><tr><td>Relationship type</td><td>Foreign Key</td></tr><tr><td>Relationship name</td><td>fk UserGroup.userGroupId to Owner.userGroupId</td></tr><tr><td>Cardinality</td><td>1</td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="93637ad0-acc0-11ea-99e3-9f008ba142d3"></a>2.1.2.7.3.5 Field **contact**

##### 2.1.2.7.3.5.1 **contact** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image134.png?raw=true)

##### 2.1.2.7.3.5.2 **contact** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>contact</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"><p>User contact info, probably email address</p></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="9e3e1af0-acc0-11ea-99e3-9f008ba142d3"></a>2.1.2.7.3.6 Field **description**

##### 2.1.2.7.3.6.1 **description** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image135.png?raw=true)

##### 2.1.2.7.3.6.2 **description** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>description</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.7.4 **Owner** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "Owner",
    "additionalProperties": false,
    "properties": {
        "ownerId": {
            "type": "string"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "userGroupId": {
            "type": "string"
        },
        "contact": {
            "type": "string",
            "description": "User contact info, probably email address"
        },
        "description": {
            "type": "string"
        }
    },
    "required": [
        "ownerId"
    ]
}
```

##### 2.1.2.7.5 **Owner** JSON data

```
{
    "ownerId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "userGroupId": "Lorem",
    "contact": "Lorem",
    "description": "Lorem"
}
```

##### 2.1.2.7.6 **Owner** Target Script

```
ottoman.model( "Owner",
{
    "ownerId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "userGroupId": {
        "type": "string"
    },
    "contact": {
        "type": "string"
    },
    "description": {
        "type": "string"
    }
}
);
```

### <a id="2498f540-acba-11ea-9dd6-9b3aa12441f0"></a>2.1.2.8 Document kind **UserGroup**

##### 2.1.2.8.1 **UserGroup** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image136.png?raw=true)

##### 2.1.2.8.2 **UserGroup** Properties

<table class="collection-properties-table"><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Document kind name</td><td>UserGroup</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Bucket</td><td><a href=#c1ca0f70-ac1f-11ea-b509-dba22c0df611>mdata</a></td></tr><tr><td>Additional properties</td><td>false</td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.8.3 **UserGroup** Fields

<table><thead><tr><td>Field</td><td>Type</td><td>Req</td><td>Key</td><td>Description</td><td>Comments</td></tr></thead><tbody><tr><td><a href=#7062eff0-acbd-11ea-9dd6-9b3aa12441f0>userGroupId</a></td><td class="no-break-word">string</td><td>true</td><td>dk</td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#d6cbddf0-acbe-11ea-99e3-9f008ba142d3>subset</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>Similar to "database" in MySQL - a subset or subdivision of the data</p></div></td><td><div class="docs-markdown"><p>METAR, MRMS, StageIV, &lt;subdatabase&gt;</p></div></td></tr><tr><td><a href=#dd11b3b0-acbe-11ea-99e3-9f008ba142d3>type</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"><p>The type of the document - similar to a table in a relational database</p></div></td><td><div class="docs-markdown"></div></td></tr><tr><td><a href=#70e76230-acbd-11ea-9dd6-9b3aa12441f0>description</a></td><td class="no-break-word">string</td><td>false</td><td></td><td><div class="docs-markdown"></div></td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="7062eff0-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.8.3.1 Field **userGroupId**

##### 2.1.2.8.3.1.1 **userGroupId** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image137.png?raw=true)

##### 2.1.2.8.3.1.2 **userGroupId** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>userGroupId</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td>true</td></tr><tr><td>Primary key</td><td>true</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="d6cbddf0-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.8.3.2 Field **subset**

##### 2.1.2.8.3.2.1 **subset** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image138.png?raw=true)

##### 2.1.2.8.3.2.2 **subset** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>subset</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/subsetId</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="dd11b3b0-acbe-11ea-99e3-9f008ba142d3"></a>2.1.2.8.3.3 Field **type**

##### 2.1.2.8.3.3.1 **type** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image139.png?raw=true)

##### 2.1.2.8.3.3.2 **type** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>type</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Activated</td><td>true</td></tr><tr><td>$ref</td><td>#model/definitions/type</td></tr><tr><td>Reference type</td><td>model</td></tr><tr><td>Reference description</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

### <a id="70e76230-acbd-11ea-9dd6-9b3aa12441f0"></a>2.1.2.8.3.4 Field **description**

##### 2.1.2.8.3.4.1 **description** Tree Diagram

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image140.png?raw=true)

##### 2.1.2.8.3.4.2 **description** properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>description</td></tr><tr><td>Technical name</td><td></td></tr><tr><td>Id</td><td></td></tr><tr><td>Type</td><td>string</td></tr><tr><td>Description</td><td><div class="docs-markdown"></div></td></tr><tr><td>Dependencies</td><td></td></tr><tr><td>Required</td><td></td></tr><tr><td>Primary key</td><td>false</td></tr><tr><td>Foreign document kind</td><td></td></tr><tr><td>Foreign field</td><td></td></tr><tr><td>Relationship type</td><td></td></tr><tr><td>Relationship name</td><td></td></tr><tr><td>Cardinality</td><td></td></tr><tr><td>Default</td><td></td></tr><tr><td>Min length</td><td></td></tr><tr><td>Max length</td><td></td></tr><tr><td>Pattern</td><td></td></tr><tr><td>Format</td><td></td></tr><tr><td>Enum</td><td></td></tr><tr><td>Sample</td><td></td></tr><tr><td>Comments</td><td><div class="docs-markdown"></div></td></tr></tbody></table>

##### 2.1.2.8.4 **UserGroup** JSON Schema

```
{
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "title": "UserGroup",
    "additionalProperties": false,
    "properties": {
        "userGroupId": {
            "type": "string"
        },
        "subset": {
            "$ref": "#model/definitions/subset"
        },
        "type": {
            "$ref": "#model/definitions/type"
        },
        "description": {
            "type": "string"
        }
    },
    "required": [
        "userGroupId"
    ]
}
```

##### 2.1.2.8.5 **UserGroup** JSON data

```
{
    "userGroupId": "Lorem",
    "subset": "Lorem",
    "type": "DataFile, LoadJob, DataSource, DataDocument, Lineage, Owner, Group",
    "description": "Lorem"
}
```

##### 2.1.2.8.6 **UserGroup** Target Script

```
ottoman.model( "UserGroup",
{
    "userGroupId": {
        "type": "string"
    },
    "subset": {
        "type": "string"
    },
    "type": {
        "type": "string"
    },
    "description": {
        "type": "string"
    }
}
);
```

### <a id="relationships"></a>

##### 3\. Relationships

### <a id="9da63c60-d670-11ea-a396-6ba9749f6a74"></a>3.1 Relationship **fk DataFile.dataFileId to DataDocument.dataFileId**

##### 3.1.1 **fk DataFile.dataFileId to DataDocument.dataFileId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td><td><a href=#1becd110-acbe-11ea-9dd6-9b3aa12441f0>dataFileId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image141.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image142.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#df2f8770-acba-11ea-9dd6-9b3aa12441f0>DataDocument</a></td><td><a href=#617c8060-d1b0-11ea-a396-6ba9749f6a74>dataFileId</a></td></tr></tbody></table>

##### 3.1.2 **fk DataFile.dataFileId to DataDocument.dataFileId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk DataFile.dataFileId to DataDocument.dataFileId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td></tr><tr><td>Parent field</td><td><a href=#1becd110-acbe-11ea-9dd6-9b3aa12441f0>dataFileId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#df2f8770-acba-11ea-9dd6-9b3aa12441f0>DataDocument</a></td></tr><tr><td>Child field</td><td><a href=#617c8060-d1b0-11ea-a396-6ba9749f6a74>dataFileId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="b814f5b0-dc05-11ea-96fb-8990ff2af80c"></a>3.2 Relationship **fk DataSource.dataSourceId to DataDocument.dataSourceId**

##### 3.2.1 **fk DataSource.dataSourceId to DataDocument.dataSourceId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0>DataSource</a></td><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image143.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image144.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#df2f8770-acba-11ea-9dd6-9b3aa12441f0>DataDocument</a></td><td><a href=#c511faa0-d283-11ea-a396-6ba9749f6a74>dataSourceId</a></td></tr></tbody></table>

##### 3.2.2 **fk DataSource.dataSourceId to DataDocument.dataSourceId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk DataSource.dataSourceId to DataDocument.dataSourceId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0>DataSource</a></td></tr><tr><td>Parent field</td><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#df2f8770-acba-11ea-9dd6-9b3aa12441f0>DataDocument</a></td></tr><tr><td>Child field</td><td><a href=#c511faa0-d283-11ea-a396-6ba9749f6a74>dataSourceId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="893cdd10-d670-11ea-a396-6ba9749f6a74"></a>3.3 Relationship **fk DataSource.dataSourceId to DataFile.dataSourceId**

##### 3.3.1 **fk DataSource.dataSourceId to DataFile.dataSourceId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0>DataSource</a></td><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image145.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image146.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td><td><a href=#964632f0-acc1-11ea-99e3-9f008ba142d3>dataSourceId</a></td></tr></tbody></table>

##### 3.3.2 **fk DataSource.dataSourceId to DataFile.dataSourceId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk DataSource.dataSourceId to DataFile.dataSourceId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#fc4f8ae0-acb9-11ea-9dd6-9b3aa12441f0>DataSource</a></td></tr><tr><td>Parent field</td><td><a href=#e36ab8c0-acbd-11ea-9dd6-9b3aa12441f0>dataSourceId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td></tr><tr><td>Child field</td><td><a href=#964632f0-acc1-11ea-99e3-9f008ba142d3>dataSourceId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="d5f27110-acc2-11ea-99e3-9f008ba142d3"></a>3.4 Relationship **fk Lineage.lineageId to LoadJob.lineageId**

##### 3.4.1 **fk Lineage.lineageId to LoadJob.lineageId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#a6423190-acb7-11ea-9dd6-9b3aa12441f0>Lineage</a></td><td><a href=#d0edcee0-acb7-11ea-9dd6-9b3aa12441f0>lineageId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image147.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image148.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#397c80e0-acaa-11ea-b509-dba22c0df611>LoadJob</a></td><td><a href=#ccd81500-acb6-11ea-9dd6-9b3aa12441f0>lineageId</a></td></tr></tbody></table>

##### 3.4.2 **fk Lineage.lineageId to LoadJob.lineageId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk Lineage.lineageId to LoadJob.lineageId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#a6423190-acb7-11ea-9dd6-9b3aa12441f0>Lineage</a></td></tr><tr><td>Parent field</td><td><a href=#d0edcee0-acb7-11ea-9dd6-9b3aa12441f0>lineageId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#397c80e0-acaa-11ea-b509-dba22c0df611>LoadJob</a></td></tr><tr><td>Child field</td><td><a href=#ccd81500-acb6-11ea-9dd6-9b3aa12441f0>lineageId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="9b671b40-acc2-11ea-99e3-9f008ba142d3"></a>3.5 Relationship **fk LoadJob.loadJobId to DataFile.loadJobId**

##### 3.5.1 **fk LoadJob.loadJobId to DataFile.loadJobId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#397c80e0-acaa-11ea-b509-dba22c0df611>LoadJob</a></td><td><a href=#7464c3c0-acaa-11ea-b509-dba22c0df611>loadJobId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image149.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image150.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td><td><a href=#6e164400-acc1-11ea-99e3-9f008ba142d3>loadJobId</a></td></tr></tbody></table>

##### 3.5.2 **fk LoadJob.loadJobId to DataFile.loadJobId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk LoadJob.loadJobId to DataFile.loadJobId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#397c80e0-acaa-11ea-b509-dba22c0df611>LoadJob</a></td></tr><tr><td>Parent field</td><td><a href=#7464c3c0-acaa-11ea-b509-dba22c0df611>loadJobId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#49dd5370-acae-11ea-9dd6-9b3aa12441f0>DataFile</a></td></tr><tr><td>Child field</td><td><a href=#6e164400-acc1-11ea-99e3-9f008ba142d3>loadJobId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="cd2051b0-d670-11ea-a396-6ba9749f6a74"></a>3.6 Relationship **fk Owner.ownerId to Lineage.ownerId**

##### 3.6.1 **fk Owner.ownerId to Lineage.ownerId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#16ae7d10-acba-11ea-9dd6-9b3aa12441f0>Owner</a></td><td><a href=#8995be30-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image151.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image152.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#a6423190-acb7-11ea-9dd6-9b3aa12441f0>Lineage</a></td><td><a href=#26b0d160-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td></tr></tbody></table>

##### 3.6.2 **fk Owner.ownerId to Lineage.ownerId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk Owner.ownerId to Lineage.ownerId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#16ae7d10-acba-11ea-9dd6-9b3aa12441f0>Owner</a></td></tr><tr><td>Parent field</td><td><a href=#8995be30-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#a6423190-acb7-11ea-9dd6-9b3aa12441f0>Lineage</a></td></tr><tr><td>Child field</td><td><a href=#26b0d160-acbd-11ea-9dd6-9b3aa12441f0>ownerId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="ca1c3970-d670-11ea-a396-6ba9749f6a74"></a>3.7 Relationship **fk UserGroup.userGroupId to Owner.userGroupId**

##### 3.7.1 **fk UserGroup.userGroupId to Owner.userGroupId** Diagram

<table><thead><tr><td>Parent Table</td><td>Parent field</td></tr></thead><tbody><tr><td><a href=#2498f540-acba-11ea-9dd6-9b3aa12441f0>UserGroup</a></td><td><a href=#7062eff0-acbd-11ea-9dd6-9b3aa12441f0>userGroupId</a></td></tr></tbody></table>

![Hackolade image](/NWP%20and%20obs%20data%20documentation/image153.png?raw=true)![Hackolade image](/NWP%20and%20obs%20data%20documentation/image154.png?raw=true)

<table><thead><tr><td>Child Table</td><td>Child field</td></tr></thead><tbody><tr><td><a href=#16ae7d10-acba-11ea-9dd6-9b3aa12441f0>Owner</a></td><td><a href=#69d9a130-acc0-11ea-99e3-9f008ba142d3>userGroupId</a></td></tr></tbody></table>

##### 3.7.2 **fk UserGroup.userGroupId to Owner.userGroupId** Properties

<table><thead><tr><td>Property</td><td>Value</td></tr></thead><tbody><tr><td>Name</td><td>fk UserGroup.userGroupId to Owner.userGroupId</td></tr><tr><td>Description</td><td></td></tr><tr><td>Parent Document kind</td><td><a href=#2498f540-acba-11ea-9dd6-9b3aa12441f0>UserGroup</a></td></tr><tr><td>Parent field</td><td><a href=#7062eff0-acbd-11ea-9dd6-9b3aa12441f0>userGroupId</a></td></tr><tr><td>Parent Cardinality</td><td>1</td></tr><tr><td>Child Document kind</td><td><a href=#16ae7d10-acba-11ea-9dd6-9b3aa12441f0>Owner</a></td></tr><tr><td>Child field</td><td><a href=#69d9a130-acc0-11ea-99e3-9f008ba142d3>userGroupId</a></td></tr><tr><td>Child Cardinality</td><td>1</td></tr><tr><td>Comments</td><td></td></tr></tbody></table>

### <a id="edges"></a>