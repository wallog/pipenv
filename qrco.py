#!/usr/bin/env python3

import qrcode

qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_L,
    box_size=10,
    border=4,
)
qr.add_data('')
qr.make(fit=True)
img = qr.make_image()
img.save("")
