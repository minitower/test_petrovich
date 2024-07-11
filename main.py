from download import FileDownloader
from tablePreprocess import TablePreprocess

url_arr = ['https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_0.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_1118258.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_1677387.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_2236516.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_2795645.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_3354774.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_3913903.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_4473032.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_5032161.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_559129.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-25_5591290.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_0.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_1068908.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_1603362.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_2137816.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_2672270.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_3206724.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_3741178.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_4275632.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_4810086.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_534454.csv',
           'https://storage.yandexcloud.net/e.tarasov/for-midle/2024-06-26_5344540.csv']
            
            
if __name__ == '__main__':
    #fd = FileDownloader(url_arr)
    #fd.download()
    tp = TablePreprocess(url_arr)
    tp.loadData()