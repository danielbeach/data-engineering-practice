from main import build_url

def test_build_url():

    #Example of data
    data=[
        ['Name', 'Last modified', 'Size', 'Description'],
        ['01002099999.csv', '2024-01-19 15:45', '178821', ''],
        ['01368099999.csv', '2024-01-19 15:45', '475362', ''],
        ['03761099999.csv', '2024-01-19 15:45', '11077920', '']
    ]

    urls = build_url(data)
    
    assert len(urls) > 0
    for url in urls:
        assert "https://" in url