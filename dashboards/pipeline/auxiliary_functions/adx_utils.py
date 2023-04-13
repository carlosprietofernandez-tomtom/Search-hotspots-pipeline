import pandas
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder


def get_adx_secrets():
    """
    Function that reads the connections.cfg file in order to access the secrets for
    connections with ASSR´s Azure Data Lake Service (ADLS) bronze, silver, and gold containers,
    which are maintained by the App Registration service.

    :return: tenant_id: Output string with the Tenant ID of TomTom.
    :return: client_id: Output string with the Client ID of the app registration.
    :return: secret_value: Empty string, since is not required, otherwise in app registration.
    :return: secret_id: Output string with the secret identifier of the app registration.
    """
    tenant_id = "374f8026-7b54-4a3a-b87d-328fa26ec10d"
    client_id = "4f8a3735-861a-4b79-9daa-9e9365b77348"
    secret_value = ""
    secret_id = "r6zvtc-Y4UJTI.2CqQN~VW-ir-suAAIk6K"
    return tenant_id, client_id, secret_value, secret_id



def execute_adx_query(query, cluster, database, client_id, secret_id, tenant_id):
    """Method to execute an adx query.

    Args:
        query ([str]): String input with Kusto format query.
        cluster ([str]): Sting input with name of cluster to connect in ADX.
        database ([str]): String input with name od database to make Kusto query in ADX.
        client_id ([str]): String input with app registry client_id value.
        secret_id ([str]): String input with app registry secret_id value.
        tenant_id ([str]): String input with app registry tenant_id value.

    Returns:
        [pandas.core.frame.DataFrame]: Output pandas dataframe with table queried in ADX.
        [azure.kusto.data.response.KustoResponseDataSetV2]: Output response of the ADX query.
    """
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, secret_id, tenant_id)
    client = KustoClient(kcsb)
    response = client.execute(database, query)
    out_df = pandas.DataFrame(data=response.tables[1].to_dict()["data"])
    return out_df, response