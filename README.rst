
What is missing
===============

``t_rinkmena`` table:

- ISTAIGA_ALT

- K_TELEFONAS

- RUSIS_ID

- RUSIS_ALT

- FORMATAS_ID

- FORMATAS_ALT

- P_DATA

- KL_P_DATA

- G_DATA

- KL_G_DATA

- ATNAUJINIMAS

- TEIKIMAS

- PATIKIMUMAS

- PATIK_PRIEZASTYS

- ISSAMUMAS

- SUKAUPTA

- PERDAVIMO_DATA

- STATUSAS

- POZYMIS

- GALIOJA

- PASTABOS

- TR_DATA

- PUB_DATA

- EKSPORTUOTI

- ALT_PAVADINIMAS

``t_istaiga`` table:

- VAD_ID

``t_user`` table:

- ID

- PASS - passwrods are encoded with md5 hash, I need to look if CKAN supports
  md5 hashed passwords. If not, then users will have to change their passwords.

- ISTAIGA_ID

- GALIOJA

- TELEFONAS

- GRUPE

- P_USER_ID

- PASSWORD_DATE

- BAD_EMAIL

- BAD_EMAIL_DATE
