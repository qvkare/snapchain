## UserDataType

Enumerates the different types of UserData.

| Name | Value | Description |
|---|---|---|
| NONE | 0 |  |
| PFP | 1 | Profile Picture |
| DISPLAY | 2 | Display Name |
| URL | 3 | URL |
| BIO | 4 | Bio |
| FNAME | 5 | Farcaster Name |

## UserDataRequest

Request to fetch UserData.

| Field | Type | Description |
|---|---|---|
| fid | uint64 | The FID of the user. |
| user_data_type | [UserDataType](#userdatatype) | The type of UserData to fetch. |

## UserDataTypeRequest

Request to fetch UserData by type with pagination.

| Field | Type | Description |
|---|---|---|
| user_data_type | [UserDataType](#userdatatype) | The type of UserData to fetch. |
| page_size | uint32 | Optional. The number of results to return per page. |
| page_token | bytes | Optional. The token for the page to return. |
| reverse | bool | Optional. Whether to return the results in reverse order. |
