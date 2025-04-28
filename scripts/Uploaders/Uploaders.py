class Uploader:
    def __init__(self, data):
        self.data = data

    def upload_csv(self, filename):
        self.data.to_csv(filename, index=False)

    def inmemory_upload(self):
        return self.data
    
    def upload_parquet(self, filename):
        self.data.to_parquet(filename, index=False)