## HubService

Provides access to Farcaster data.

### GetUserData

Returns a single Message for a given UserDataRequest.

| Field | Type | Description |
|---|---|---|
| request | [UserDataRequest](#userdatarequest) | The request containing the FID and UserDataType. |
| returns | [Message](#message) | The Message containing the UserData, or an error if not found. |

### GetUserDataByFid

Returns all UserData Messages for a given FidRequest.

| Field | Type | Description |
|---|---|---|
| request | [FidRequest](#fidrequest) | The request containing the FID. |
| returns | [MessagesResponse](#messagesresponse) | A list of Messages containing UserData. |

### GetUserDataByType

Returns UserData Messages for a given UserDataTypeRequest.

| Field | Type | Description |
|---|---|---|
| request | [UserDataTypeRequest](#userdatatyperequest) | The request containing the UserDataType and pagination parameters. |
| returns | [MessagesResponse](#messagesresponse) | A list of Messages containing UserData. |

### GetUsernameProof

Returns a UsernameProof for a given UsernameProofRequest.

| Field | Type | Description |
|---|---|---|
| request | [UsernameProofRequest](#usernameproofrequest) | The request containing the username. |
| returns | [UserNameProof](#usernameproof) | The UsernameProof, or an error if not found. |
