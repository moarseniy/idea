apt-get install ffmpeg
apt-get install tesseract-ocr
apt-get install tesseract-ocr-rus
apt-get install -y poppler-utils
apt-get install postgresql
bash create_db.sh

python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
