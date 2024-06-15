import json
import base64
import subprocess
import os
import tempfile
import boto3
from PIL import Image
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

s3_client = boto3.client('s3')
BUCKET_NAME = os.environ['BUCKET_NAME']

def lambda_handler(event, context):

    try:
        body = base64.b64decode(event['body'])

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
            temp_pdf_path = temp_pdf.name
            temp_pdf.write(body)

        temp_image_dir = tempfile.mkdtemp()

        try:
            command = f"pdfimages -j {temp_pdf_path} {temp_image_dir}/image"
            subprocess.run(command, shell=True, check=True)

            images = [f for f in os.listdir(temp_image_dir) if f.lower().endswith(('.pbm', '.jpeg'))]

            image_urls = []
            for image_file in images:
                image_path = os.path.join(temp_image_dir, image_file)
                jpeg_image_path = os.path.join(temp_image_dir, f"{os.path.splitext(image_file)[0]}.jpeg")

                try:
                    with Image.open(image_path) as img:
                        img.convert('RGB').save(jpeg_image_path, 'JPEG')
                except Exception as e:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': f"Error converting image {image_file} to JPEG: {str(e)}"})
                    }

                s3_key = f"pdf_images/{os.path.basename(jpeg_image_path)}"
                
                try:
                    s3_client.upload_file(jpeg_image_path, BUCKET_NAME, s3_key)
                    # Gera URL assinada com TTL de 5 minutos
                    image_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': BUCKET_NAME, 'Key': s3_key}, ExpiresIn=300)
                    image_urls.append(image_url)
                except (NoCredentialsError, PartialCredentialsError) as e:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': str(e)})
                    }

            return {
                'statusCode': 200,
                'body': json.dumps({'image_urls': image_urls})
            }
        
        finally:
            os.remove(temp_pdf_path)
            for image_name in os.listdir(temp_image_dir):
                os.remove(os.path.join(temp_image_dir, image_name))
            os.rmdir(temp_image_dir)

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Erro ao processar o PDF",
                "error": str(e)
            }),
            "headers": {
                "Content-Type": "application/json"
            }
        }
