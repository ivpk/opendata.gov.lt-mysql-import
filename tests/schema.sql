PRAGMA synchronous = OFF;
PRAGMA journal_mode = MEMORY;
BEGIN TRANSACTION;
CREATE TABLE `email_list` (
  `id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT
,  `time` integer  NOT NULL
,  `user_id` integer  NOT NULL
,  `email` varchar(255) NOT NULL
,  `extras` varchar(255) NOT NULL
);
CREATE TABLE `ezsession_preferences` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `userid` integer NOT NULL DEFAULT '0'
,  `name` char(255) DEFAULT NULL
,  `vvalue` char(255) DEFAULT NULL
,  `groupname` char(255) DEFAULT NULL
);
CREATE TABLE `ezsession_session` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `hash` char(33) NOT NULL DEFAULT ''
,  `created` integer NOT NULL DEFAULT '0'
,  `lastaccessed` integer NOT NULL DEFAULT '0'
);
CREATE TABLE `ezsession_sessionvariable` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `sessionid` integer NOT NULL DEFAULT '0'
,  `name` varchar(255) NOT NULL
,  `vvalue` text COLLATE BINARY
,  `groupname` varchar(255) DEFAULT NULL
);
CREATE TABLE `lt_pastabos` (
  `r_kodas` integer  NOT NULL
,  `r_id` integer  NOT NULL
,  `r_pavadinimas` varchar(250) NOT NULL
,  `problema` text NOT NULL
,  `pastaba` text NOT NULL
,  `statusas` varchar(12) NOT NULL
,  `u_id` integer  NOT NULL
,  `i_id` integer  NOT NULL
,  `emails` text NOT NULL
,  `sm` integer  NOT NULL DEFAULT '0'
,  PRIMARY KEY (`r_kodas`)
);
CREATE TABLE `mass_mail` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `date` timestamp NOT NULL 
,  `user_id` integer NOT NULL
,  `subject` varchar(300) DEFAULT NULL
,  `message` text NOT NULL
);
CREATE TABLE `mass_mail_recipients` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `mass_mail_id` integer NOT NULL
,  `user_id` integer NOT NULL
,  `sent` integer NOT NULL DEFAULT '0'
);
CREATE TABLE `questions` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `date` timestamp NOT NULL 
,  `ip` varchar(20) DEFAULT NULL
,  `category_id` integer DEFAULT NULL
,  `question` text
,  `answer` text
,  `visible` integer DEFAULT '0'
,  `email` varchar(200) NOT NULL
,  UNIQUE (`id`)
);
CREATE TABLE `questions_categories` (
  `id` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `name` varchar(200) NOT NULL
,  UNIQUE (`id`)
);
CREATE TABLE `t_event_log` (
  `id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT
,  `rinkmena_id` integer  NOT NULL
,  `r_kodas` integer  NOT NULL
,  `r_pavadinimas` varchar(255) NOT NULL
,  `istaiga` integer  NOT NULL
,  `kada` datetime NOT NULL
,  `user_id` integer  NOT NULL
,  `event` integer  NOT NULL
,  `extras` text NOT NULL
);
CREATE TABLE `t_forgot` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `UserID` integer DEFAULT NULL
,  `Hash` varchar(50) DEFAULT NULL
,  `Time` timestamp NOT NULL 
,  UNIQUE (`ID`)
);
CREATE TABLE `t_formatas` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `PAVADINIMAS` varchar(255) NOT NULL
);
CREATE TABLE `t_istaiga` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `KODAS` varchar(255) NOT NULL
,  `PAVADINIMAS` varchar(255) NOT NULL
,  `ADRESAS` varchar(255) NOT NULL
,  `VAD_ID` integer DEFAULT NULL
);
CREATE TABLE `t_kategorija` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `PAVADINIMAS` varchar(255) NOT NULL
,  `KATEGORIJA_ID` integer DEFAULT NULL
,  `LYGIS` integer NOT NULL DEFAULT '1'
);
CREATE TABLE `t_kategorija_rinkmena` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `KATEGORIJA_ID` integer NOT NULL DEFAULT '0'
,  `RINKMENA_ID` integer NOT NULL DEFAULT '0'
);
CREATE TABLE `t_rinkmena` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `KODAS` integer NOT NULL DEFAULT '0'
,  `PAVADINIMAS` varchar(255) DEFAULT NULL
,  `ALT_PAVADINIMAS` varchar(255) DEFAULT NULL
,  `SANTRAUKA` text COLLATE BINARY
,  `R_ZODZIAI` varchar(255) DEFAULT NULL
,  `USER_ID` integer DEFAULT NULL
,  `istaiga_id` integer DEFAULT NULL
,  `ISTAIGA_ALT` varchar(255) DEFAULT NULL
,  `K_TELEFONAS` text COLLATE BINARY
,  `K_EMAIL` text COLLATE BINARY
,  `RUSIS_ID` integer DEFAULT NULL
,  `RUSIS_ALT` varchar(255) DEFAULT NULL
,  `FORMATAS_ID` varchar(255) DEFAULT NULL
,  `FORMATAS_ALT` varchar(255) DEFAULT NULL
,  `P_DATA` integer DEFAULT NULL
,  `KL_P_DATA` varchar(12) DEFAULT NULL
,  `G_DATA` integer DEFAULT NULL
,  `KL_G_DATA` varchar(12) DEFAULT NULL
,  `ATNAUJINIMAS` varchar(255) DEFAULT NULL
,  `TINKLAPIS` text COLLATE BINARY
,  `TEIKIMAS` text COLLATE BINARY
,  `PATIKIMUMAS` char(1) DEFAULT NULL
,  `PATIK_PRIEZASTYS` varchar(255) DEFAULT NULL
,  `ISSAMUMAS` char(1) DEFAULT NULL
,  `SUKAUPTA` varchar(255) DEFAULT NULL
,  `PERDAVIMO_DATA` datetime DEFAULT '0000-00-00 00:00:00'
,  `STATUSAS` char(1) NOT NULL DEFAULT ''
,  `POZYMIS` char(1) NOT NULL DEFAULT ''
,  `GALIOJA` char(1) NOT NULL DEFAULT ''
,  `PASTABOS` text COLLATE BINARY
,  `TR_DATA` datetime DEFAULT NULL
,  `PUB_DATA` datetime DEFAULT NULL
,  `EKSPORTUOTI` integer NOT NULL DEFAULT '1'
);
CREATE TABLE `t_rinkmenu_logas` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `DATA` datetime NOT NULL
,  `NAUJAS` integer DEFAULT NULL
,  `TIPAS` varchar(30) DEFAULT NULL
,  `REIKSME` text COLLATE BINARY
,  `SENA_REIKSME` text COLLATE BINARY
,  `RINKMENOS_ID` integer NOT NULL
);
CREATE TABLE `t_rusis` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `PAVADINIMAS` varchar(255) NOT NULL
);
CREATE TABLE `t_user` (
  `ID` integer NOT NULL PRIMARY KEY AUTOINCREMENT
,  `LOGIN` varchar(255) NOT NULL
,  `PASS` varchar(32) NOT NULL DEFAULT ''
,  `FIRST_NAME` varchar(255) NOT NULL
,  `LAST_NAME` varchar(255) NOT NULL
,  `ISTAIGA_ID` integer NOT NULL DEFAULT '0'
,  `GALIOJA` char(1) NOT NULL DEFAULT ''
,  `EMAIL` varchar(255) NOT NULL
,  `TELEFONAS` varchar(255) NOT NULL
,  `GRUPE` char(1) NOT NULL DEFAULT ''
,  `P_USER_ID` varchar(100) DEFAULT NULL
,  `PASSWORD_DATE` datetime DEFAULT NULL
,  `BAD_EMAIL` integer NOT NULL DEFAULT '0'
,  `BAD_EMAIL_DATE` datetime DEFAULT NULL
);
CREATE TABLE `t_uzklausimai` (
  `uzklausimo_id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT
,  `vardas` varchar(255) NOT NULL
,  `pavarde` varchar(255) NOT NULL
,  `organizacija` varchar(255) NOT NULL
,  `tel` varchar(255) NOT NULL
,  `email` varchar(255) NOT NULL
,  `kam` integer  NOT NULL
,  `rinkmena` integer  NOT NULL
,  `istaiga` integer  NOT NULL
,  `persiunte` integer  NOT NULL
,  `klausimo_data` datetime NOT NULL
,  `antraste` varchar(255) NOT NULL
,  `klausimas` text NOT NULL
,  `pastaba` integer  NOT NULL
);
CREATE TABLE `t_uzklausimu_zinutes` (
  `zinutes_id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT
,  `uzklausimo_id` integer  NOT NULL
,  `atsake` integer  NOT NULL
,  `atsakymo_data` datetime NOT NULL
,  `atsakymas` text NOT NULL
);
CREATE TABLE `url_validation_jobs` (
  `id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT
,  `created_by` integer  NOT NULL
,  `created_on` datetime NOT NULL
);
CREATE TABLE `url_validation_links` (
  `id` integer  NOT NULL PRIMARY KEY AUTOINCREMENT
,  `job_id` integer  NOT NULL
,  `url` varchar(255) NOT NULL
,  `rinkmena_id` integer  NOT NULL
,  `rinkmena_kodas` integer  NOT NULL
,  `rinkmena_pavadinimas` varchar(255) NOT NULL
,  `rinkmena_istaiga` integer  NOT NULL
,  `status` integer  NOT NULL
,  `syntax_check` integer  DEFAULT NULL
,  `server_response` integer  DEFAULT NULL
,  `checked_on` datetime DEFAULT NULL
,  `answer` varchar(255) DEFAULT NULL
,  `answer_match_percentage` integer DEFAULT NULL
);
CREATE INDEX "idx_t_event_log_rinkmena_id" ON "t_event_log" (`rinkmena_id`);
CREATE INDEX "idx_t_uzklausimu_zinutes_uzklausimo_id" ON "t_uzklausimu_zinutes" (`uzklausimo_id`);
CREATE INDEX "idx_t_uzklausimai_kam" ON "t_uzklausimai" (`kam`);
CREATE INDEX "idx_t_uzklausimai_rinkmena" ON "t_uzklausimai" (`rinkmena`);
CREATE INDEX "idx_t_uzklausimai_istaiga" ON "t_uzklausimai" (`istaiga`);
CREATE INDEX "idx_t_uzklausimai_pastaba" ON "t_uzklausimai" (`pastaba`);
CREATE INDEX "idx_url_validation_links_job_id" ON "url_validation_links" (`job_id`);
CREATE INDEX "idx_url_validation_links_status" ON "url_validation_links" (`status`);
CREATE INDEX "idx_email_list_time" ON "email_list" (`time`);
CREATE INDEX "idx_email_list_user_id" ON "email_list" (`user_id`);
CREATE INDEX "idx_email_list_email" ON "email_list" (`email`);
END TRANSACTION;
