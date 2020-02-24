// curl -X GET  http://localhost:8080/dataavenue/rest/authentication/s3
// [{"displayName": "S3 authentication",
// "type": "UserPass",
// "fields": [{"keyName": "UserID", "displayName": "Access key"}, {"keyName": "UserPass", "displayName": "Secret key"}]}]

import {AuthField} from './authfield';

export class Auth {
  type: string; // UserPass UserID (access key), UserPass (secret key)
  displayName: string;
  fields: AuthField[]; // JSON fields
  xcredentials: string;
  url: string;
  isAuthenticated: boolean;
}
