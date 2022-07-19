// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql_db

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	jose "gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

var errKeyNotFound = errors.New("Key not found")
var errFileNotFound = errors.New("file not found")

func validateJWT(config []JwksConfig, username, identity, token string) (bool, error) {
	// Verify that the JWKS for the configured authentication exists.
	if len(config) == 0 {
		return false, nil
	}
	parsed, err := jwt.ParseSigned(token)
	if err != nil {
		return false, err
	}
	if len(parsed.Headers) != 1 {
		return false, fmt.Errorf("ValidateJWT: Unexpected JWT headers length %v.", len(parsed.Headers))
	}
	parsedIdentity := parseUserIdentity(identity)
	jwksConfig, err := getMatchingJwksConfig(config, parsedIdentity["jwks"])
	if err != nil {
		return false, err
	}
	if parsedIdentity["sub"] != username {
		return false, fmt.Errorf("ValidateJWT: Subjects do not match")
	}
	// Verify that the alg in the incoming JWT matches the alg config in the JWKS.
	if jwksConfig.Claims["alg"] != parsed.Headers[0].Algorithm {
		return false, fmt.Errorf("ValidateJWT: Algorithms do not match")
	}
	// Verify the signature of the JWT using the JWKS contents fetched from the configured location.
	fetchedJwks, err := getJWKSFromSource(jwksConfig.Source)
	if err != nil {
		return false, err
	}
	keyID := parsed.Headers[0].KeyID
	keys := fetchedJwks.Key(keyID)

	var claims jwt.Claims
	claimsError := fmt.Errorf("ValidateJWT: KeyID: %v. Err: %w", keyID, errKeyNotFound)
	for _, key := range keys {
		claimsError = parsed.Claims(key.Key, &claims)
		if claimsError == nil {
			break
		}
	}
	if claimsError != nil {
		return false, claimsError
	}

	// Verify that the signed JWT is not expired and does not violate "nbf"
	// Verify all claims on the JWT as configured in the USER and the JWKS.
	expectedClaims := getExpectedClaims(jwksConfig.Claims)
	if err := claims.Validate(expectedClaims.WithTime(time.Now())); err != nil {
		return false, err
	}

	logString := "Authenticating with JWT: "
	for _, field := range jwksConfig.FieldsToLog {
		logString = logString + fmt.Sprintf("%s: %s,", field, getClaimFromKey(claims, field))
	}
	logrus.Info(logString)
	return true, nil
}

func getClaimFromKey(claims jwt.Claims, field string) string {
	switch field {
	case "id":
		return claims.ID
	case "iss":
		return claims.Issuer
	case "sub":
		return claims.Subject
	}
	return ""
}

func getExpectedClaims(claims map[string]string) jwt.Expected {
	var expectedClaims jwt.Expected
	for cl, value := range claims {
		switch cl {
		case "iss":
			expectedClaims.Issuer = value
		case "sub":
			expectedClaims.Subject = value
		case "aud":
			expectedClaims.Audience = []string{value}
		}
	}
	return expectedClaims
}

func getMatchingJwksConfig(config []JwksConfig, name string) (*JwksConfig, error) {
	for _, item := range config {
		if item.Name == name {
			return &item, nil
		}
	}
	return nil, fmt.Errorf("ValidateJWT: Matching JWKS config not found")
}

func getJWKSFromFile(filepath string) (*jose.JSONWebKeySet, error) {

	file, err := os.Open(filepath)
	if err != nil || file == nil {
		return nil, errFileNotFound
	}
	defer file.Close()

	byteValue, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return unmarshalAndConvertKeys(byteValue)
}

func getJWKSFromUrl(locationUrl string) (*jose.JSONWebKeySet, error) {
	client := &http.Client{}

	request, err := http.NewRequest("GET", locationUrl, nil)
	if err != nil {
		return nil, err
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, err
	} else if response.StatusCode/100 != 2 {
		return nil, errors.New("FetchedJWKS: Non-2xx status code from JWKS fetch")
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}

		return unmarshalAndConvertKeys(contents)
	}
}

type jsonKey struct {
	ID  string `json:"id"`
	Key string `json:"key_bytes"`
}

func unmarshalAndConvertKeys(byteValue []byte) (*jose.JSONWebKeySet, error) {
	jsonKeys := []jsonKey{}
	err := json.Unmarshal(byteValue, &jsonKeys)
	if err != nil {
		return nil, err
	}

	keys := make([]jose.JSONWebKey, len(jsonKeys))
	for i, jKey := range jsonKeys {
		priv, err := decodePrivateKey(jKey.Key)
		if err != nil {
			return nil, err
		}
		pub := priv.Public()
		keys[i] = jose.JSONWebKey{
			KeyID: jKey.ID,
			Key:   pub,
		}
	}

	return &jose.JSONWebKeySet{Keys: keys}, nil
}

func decodePrivateKey(key string) (*rsa.PrivateKey, error) {
	dec, err := base32.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, err
	}
	privKey, err := x509.ParsePKCS8PrivateKey(dec)
	if err != nil {
		return nil, err
	}
	k, ok := privKey.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("could not convert to rsa.PrivateKey")
	}
	return k, nil
}

func getJWKSFromSource(source string) (*jose.JSONWebKeySet, error) {
	// Check if file exists, otherwise get from url
	jwks, err := getJWKSFromFile(source)
	if err != nil {
		if errors.Is(err, errFileNotFound) {
			jwks, err = getJWKSFromUrl(source)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return jwks, nil
}

func parseUserIdentity(identity string) map[string]string {
	idMap := make(map[string]string)
	items := strings.Split(identity, ",")
	for _, item := range items {
		tup := strings.Split(item, "=")
		idMap[tup[0]] = tup[1]
	}
	return idMap
}
