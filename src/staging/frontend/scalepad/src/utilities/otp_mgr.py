import pyotp
from pyzbar.pyzbar import decode
from PIL import Image


class GenerateOTP:
    def __init__(self):
        pass

    @staticmethod
    def generate_otp_from_secret(secret_key):
        totp = pyotp.TOTP(secret_key).now()

        if totp:
            print('Generated OTP:', totp)
            return totp

    @staticmethod
    def scan_codes(image_path):
        """Scans a QR code from an image file and extracts the data."""
        with open(image_path, 'rb') as image_file:
            image = Image.open(image_file)
            decoded_data = decode(image)

            if decoded_data:
                code_data = [(d.type, d.data.decode('utf-8')) for d in decoded_data]
                return code_data

        return None


    @staticmethod
    def generate_otp_from_image(image_path):
        """Scans a QR code from an image file and extracts the data."""
        with open(image_path, 'rb') as image_file:
            image = Image.open(image_file)
            decoded_data = decode(image)

            if decoded_data:
                code_data = [(d.type, d.data.decode('utf-8')) for d in decoded_data]

                print('Scanned Codes:')
                for barcode_type, barcode_data in code_data:
                    print('Barcode Type:', barcode_type)
                    print('Barcode Data:', barcode_data)

                totp = otp_generator.generate_otp_from_uri(code_data[0][1])

                if totp:
                    print('Generated OTP:', totp)
                    return totp
            else:
                print('No barcodes or QR codes found in the image.')


    @staticmethod
    def generate_otp_from_uri(uri):
        """Generates a one-time password (OTP) from a given URI string."""
        try:
            otp_data = pyotp.parse_uri(uri)
            return otp_data.now()
        except Exception as e:
            print(f"Error parsing OTP URI: {e}")
            return None


if __name__ == '__main__':
    otp_generator = GenerateOTP()
    img = './data/qr_code.png'  # Replace with your barcode/QR code image path

    data = otp_generator.scan_codes(image_path=img)
