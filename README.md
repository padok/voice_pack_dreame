# GLaDOS Voice Pack for Dreame Vacuum Robots

This project provides a Portal‑themed sarcastic GLaDOS voice pack for Dreame‑based vacuum robots, generated using 👉 [glados.c-net.org](https://glados.c-net.org/).
The prebuilt archive can be used directly in Valetudo.

MD5 sum of the prepackaged `voice_pack.tar.gz`:  
`7a92bb665d7b74fcf94df95d2ed579db`

Tested and confirmed working on:

- L10 Pro
- L10S Pro Ultra Heat
- Z10 Pro
- W10
- D9

## 🚀 Installation (via Valetudo)

1. In Valetudo go to "Robot Settings" -> "Misc Settings"
1. Enter the following information in the "Voice packs" section:
    - URL: `https://github.com/padok/voice_pack_dreame/raw/main/voice_pack.tar.gz`
    - Language Code: `GLADOS`
    - Hash: `7a92bb665d7b74fcf94df95d2ed579db`
    - File size: `8321365` bytes
1. Click "Set Voice Pack"

## 🛠️ Development

This project includes scripts that automatically generate and package the audio files.

To build everything:

```shell
poetry install
poetry run glados-audio
poetry run make-release
```

This will:

- Download/generate all GLaDOS audio files
- Encode them to OGG
- Build the voice_pack.tar.gz release file


## 🙏 Thanks
Special thanks to:

https://glados.c-net.org/ — for the amazing voice generation
Findus23: https://github.com/Findus23/voice_pack_dreame
ccoors: https://github.com/ccoors/dreame_voice_packs

Both provided inspiration and original lists of Dreame sound IDs.