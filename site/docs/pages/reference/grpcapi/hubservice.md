## HubService

The `HubService` provides access to Farcaster data.

### Methods

| Method                     | Request                | Response             | Description                                                                 |
| -------------------------- | ---------------------- | -------------------- | --------------------------------------------------------------------------- |
| GetInfo                    | GetInfoRequest         | GetInfoResponse      | Returns information about the Hub.                                          |
| GetHubState                | GetHubStateRequest     | HubState             | Returns the Hub's current state.                                            |
| GetMessage                 | GetMessageRequest      | Message              | Returns a specific message.                                                 |
| GetAllCastMessages         | GetAllCastMessagesRequest | MessagesResponse     | Returns all cast messages.                                                  |
| GetAllReactionMessages     | GetAllReactionMessagesRequest | MessagesResponse     | Returns all reaction messages.                                              |
| GetAllSignerMessages       | GetAllSignerMessagesRequest | MessagesResponse     | Returns all signer messages.                                                |
| GetAllUserDataMessages     | GetAllUserDataMessagesRequest | MessagesResponse     | Returns all user data messages.                                             |
| GetCastsByFid              | FidRequest             | MessagesResponse     | Returns casts by a specific Fid.                                            |
| GetCastsByParent           | CastsByParentRequest   | MessagesResponse     | Returns casts by a specific parent.                                         |
| GetCastsByMention          | FidRequest             | MessagesResponse     | Returns casts that mention a specific Fid.                                  |
| GetCastsByHashtag          | HashtagRequest         | MessagesResponse     | Returns casts that contain a specific hashtag.                               |
| GetReaction                | ReactionRequest        | Message              | Returns a specific reaction.                                                |
| GetReactionsByTarget       | ReactionsByTargetRequest | MessagesResponse     | Returns reactions for a specific target.                                      |
| GetReactionsByCast         | CastId                 | MessagesResponse     | Returns reactions for a specific cast.                                        |
| GetReactionsByFid          | FidRequest             | MessagesResponse     | Returns reactions by a specific Fid.                                         |
| GetUserData                | UserDataRequest        | Message              | Returns a specific user data message.                                       |
| GetUserDataByFid           | FidRequest             | MessagesResponse     | Returns user data messages for a specific Fid.                               |
| GetSigner                  | SignerRequest          | Message              | Returns a specific signer message.                                           |
| GetSignersByFid            | FidRequest             | MessagesResponse     | Returns signer messages for a specific Fid.                                  |
| GetIdRegistryEvent         | IdRegistryEventRequest | OnChainEvent         | Returns a specific Id Registry event.                                        |
| GetIdRegistryEventsByFid   | FidRequest             | OnChainEventsResponse | Returns Id Registry events for a specific Fid.                               |
| GetNameRegistryEvent       | NameRegistryEventRequest | OnChainEvent         | Returns a specific Name Registry event.                                      |
| GetNameRegistryEventsByFid | FidRequest             | OnChainEventsResponse | Returns Name Registry events for a specific Fid.                              |
| GetVerification            | VerificationRequest      | Message              | Returns a specific verification message.                                      |
| GetVerificationsByFid      | FidRequest             | MessagesResponse     | Returns verification messages for a specific Fid.                               |
| GetLink                    | LinkRequest            | Message              | Returns a specific link message.                                             |
| GetLinksByTarget           | LinksByTargetRequest   | MessagesResponse     | Returns link messages for a specific target.                                   |
| GetLinksByFid              | FidRequest             | MessagesResponse     | Returns link messages for a specific Fid.                                      |
| Subscribe                  | SubscribeRequest       | stream HubEvent      | Subscribes to a stream of HubEvents.                                        |
| GetSyncStatus              | GetSyncStatusRequest   | SyncStatusResponse   | Returns the sync status of the Hub.                                         |
| GetSyncMetadataByPrefix    | GetSyncMetadataRequest | SyncMetadataResponse | Returns sync metadata for a specific prefix.                                  |
| GetSyncTrieNodesByPrefix | GetSyncTrieNodesRequest | SyncTrieNodesResponse | Returns sync trie nodes for a specific prefix.                               |
