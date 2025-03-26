package Factory

import (
    "nzsyncbackup/connector"
)

func GetConnector(connectorType string, azconnect *Connector.AZConnector, s3connect *Connector.S3connector) Connector.IConnector {
    switch connectorType {
    case "aws":
        return s3connect
    case "azure":
        return azconnect
    default:
        return nil
    }
}

