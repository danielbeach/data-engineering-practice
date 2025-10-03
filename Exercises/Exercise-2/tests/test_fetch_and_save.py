import pytest
from main import fetch_and_save
from unittest.mock import patch,MagicMock,AsyncMock 

# def test_fetch_and_save(tmp_path):
#     #arrange
#     output_file=tmp_path / "page_content.txt"

#     #create a fake response object
#     fake_response = MagicMock()
#     fake_response.status_code = 200
#     fake_response.text = "<html><body>Hello Test</body></html>"

#     #patch requests.get to return our fake response
#     with patch("main.requests.get",return_value=fake_response):
#         success=fetch_and_save("http://fakeurl.com",str(output_file))

#     assert success is True 
#     assert output_file.exists()
#     content = output_file.read_text(encoding="utf-8")
#     assert "Hello Test" in content

# def test_fetch_and_save_fail(tmp_path):
#     #arrange
#     output_file=tmp_path / "page_content.txt"

#     #create a fake response object
#     fake_response = MagicMock()
#     fake_response.status_code = 404
#     fake_response.text = "Not Found"

#     #patch requests.get to return our fake response
#     with patch("main.requests.get",return_value=fake_response):
#         success=fetch_and_save("http://fakeurl.com",str(output_file))

#     assert success is False 
#     assert not output_file.exists()

#@pytest.mark.asyncio
# @pytest.mark.parametrize(
#     "status_code,expected_success,should_exist",
#     [
#         (200,True,True), #success case
#         (404,False,False), #failure: Not Found
#         (500,False,False) #failure: server error
#     ]
# )
# def test_fetch_and_save_fail_parametrize(tmp_path,status_code,expected_success,should_exist):
#     arrange
#     output_file=tmp_path / "urls_list.txt"

#     create a fake response object
#     fake_response = MagicMock()
#     fake_response.status_code = status_code
#     fake_response.text = f"Response{status_code}"
#     htlm_doc="""
#     <html>
#     <body>
#         <table>
#             <thead>
#                 <tr><th>Name</th><th>Last modified</th></tr>
#             </thead>
#             <tbody>
#                 <tr><td>Data1.csv</td><td>2024-01-19 14:51</td></tr>
#                 <tr><td>Data2.csv</td><td>2024-01-19 14:51</td></tr>
#             </tbody>
#         </table>
#     </body>
#     </html>
#     """
#     fake_response.text = htlm_doc

#     patch requests.get to return our fake response
#     with patch("main.requests.get",return_value=fake_response):
#     with patch("main.aiohttp.ClientSession",return_value=fake_response):
#         success= fetch_and_save("http://fakeurl.com",str(output_file))

#     assert success == expected_success 
#     assert output_file.exists() == should_exist

#     if should_exist:
#         assert "https://" in output_file.read_text(encoding="utf-8")