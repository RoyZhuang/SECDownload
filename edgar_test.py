import datetime

from SECDownload import settings, edgarcik


if __name__ == "__main__":
    # Configuration data
    testing = False
    config = settings.from_file(
        settings.DEFAULT_CONFIG_FILENAME, testing
    )

    print(config)
    
    test = edgarcik.EdgarHandler(
        edgar_dir=config.EDGAR_DATA_DIR,
        cik_dir=config.CIK_DIR,
        cik_merged_dir=config.CIK_MERGED_DIR,
        start_year=2017)

    test.GetEdgarIndex()
    test.CIKDownload()
    test.CIKMerge()

    print('Finish Test')
