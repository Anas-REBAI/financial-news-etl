from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration des secrets client pour Google Drive
CLIENT_SECRETS_JSON = {
    "installed": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "redirect_uris": ["http://localhost"]
    }
}

# Créer un fichier JSON temporaire avec les secrets
with open("client_secrets.json", "w") as f:
    json.dump(CLIENT_SECRETS_JSON, f)

# 🔥 Authentification Google Drive
gauth = GoogleAuth()

# Charger la configuration à partir du fichier JSON
gauth.LoadClientConfigFile("client_secrets.json")  # Utilisation du fichier JSON
gauth.LocalWebserverAuth()  # Ouvre une page pour autoriser l'accès

# Supprimer le fichier JSON temporaire après utilisation
os.remove("client_secrets.json")

# 📂 Connexion à Google Drive
drive = GoogleDrive(gauth)

# 📄 Création d'un fichier test
file_drive = drive.CreateFile({'title': 'test_upload.txt'})
file_drive.SetContentString("Ceci est un test d'upload avec PyDrive2.")
file_drive.Upload()

print(f"✅ Fichier test uploadé avec succès : {file_drive['alternateLink']}")