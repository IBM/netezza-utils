package Factory

import (
    "nzconnector/connector"
)

func GetConnector(connectorType string) Connector.IConnector {
    switch connectorType {
    case "s3":
        return &Connector.S3connector{}
    case "az":
        return &Connector.AZConnector{}
    default:
        return nil
    }
}

