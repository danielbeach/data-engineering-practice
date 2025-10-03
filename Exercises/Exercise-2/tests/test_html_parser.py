from main import parse_html

def test_parse_html(tmp_path):
    #Example HTML
    html_doc = """
    <html>
    <body>
        <table>
            <thead>
                <tr><th>Header 1</th><th>Header 2</th></tr>
            </thead>
            <tbody>
                <tr><td>Data 1A</td><td>Data 1B</td></tr>
                <tr><td>Data 2A</td><td>Data 2B</td></tr>
            </tbody>
        </table>
    </body>
    </html>
    """

    data = parse_html(html_doc)

    assert len(data) > 0
