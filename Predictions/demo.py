import pyarrow.fs as fs

hdfs = fs.HadoopFileSystem('localhost', 9000)
print(hdfs.get_file_info(fs.FileSelector('/user/amitk/cleaned_stock_data', recursive=True)))
