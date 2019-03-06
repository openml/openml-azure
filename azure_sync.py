import openml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import jsonpickle
import configparser
from azure.storage.blob import BlockBlobService, PublicAccess

def sync_azure():

    def create_meta_data(openml_data):
        for key in ['data_file', 'data_pickle_file', 'format',
                    'md5_checksum', '_dataset', 'url']:
            delattr(openml_data, key)
        jsondata = jsonpickle.encode(openml_data, unpicklable=False)
        return jsondata

    def upload(container_name, file_name, file):
        print('Syncing', file_name)
        block_blob_service.create_blob_from_path(container_name, file_name, file)
        url = block_blob_service.make_blob_url(container_name,file_name)
        print(file_name, ':', url)
        return url

    try:
        # Load the account name and key from the config file:
        config = configparser.ConfigParser()
        config.read('config.ini')

        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlockBlobService(account_name=config['Azure']['account_name'],
                                              account_key=config['Azure']['account_key'])
        container_name ='openml'

        # List the blobs currently in the container
        uploaded_datasets = [b.name for b in block_blob_service.list_blobs(container_name)]
        print(uploaded_datasets)

        # Fetch tagged datasets from OpenML
        datasets = openml.datasets.list_datasets(tag="AzurePilot")
        for d in datasets.values():
            data_id = d['did']

            # Sync if not yet uploaded
            if data_id not in uploaded_datasets:
                # Get data
                dataset = openml.datasets.get_dataset(d['did'])

                # Sync ARFF
                arff_url = upload(container_name, '{0}/arff/data-{0}.arff'.format(data_id), dataset.data_file)
                dataset.arff_url = arff_url

                # Sync Parquet
                X, y, attribute_names = dataset.get_data(
                    target=dataset.default_target_attribute,
                    return_attribute_names=True)
                df = pd.DataFrame(X, columns=attribute_names)
                df[dataset.default_target_attribute] = y
                table = pa.Table.from_pandas(df)
                pq.write_table(table, 'data.parquet')
                parquet_url = upload(container_name, '{0}/parquet/data-{0}.parquet'.format(data_id), 'data.parquet')
                dataset.parquet_url = parquet_url

                # Build meta-data
                jsondata = create_meta_data(dataset)
                with open('metadata.json', 'w') as f:
                    f.write(jsondata)
                upload(container_name, str(data_id) + '/metadata.json', 'metadata.json')

    except Exception as e:
        print(e)


# Main method.
if __name__ == '__main__':
    sync_azure()



