from flask import Flask, make_response

app = Flask(__name__)

TRANSPARENT_GIF = (
    b'GIF89a\x01\x00\x01\x00\x80\x00\x00'
    b'\xff\xff\xff\x00\x00\x00!\xf9\x04'
    b'\x00\x00\x00\x00\x00,\x00\x00\x00'
    b'\x00\x01\x00\x01\x00\x00\x02\x02'
    b'D\x01\x00;'
)

@app.route('/pixels/<path:pixel_name>')
def serve_pixel(pixel_name):
    response = make_response(TRANSPARENT_GIF)
    response.headers['Content-Type']  = 'image/gif'
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    response.headers['Pragma']        = 'no-cache'
    response.headers['Expires']       = '0'
    return response
