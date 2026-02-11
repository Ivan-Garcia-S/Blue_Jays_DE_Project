CREATE TABLE game (
    gamepk INT PRIMARY KEY,
    gamedate TIMESTAMP,
    officialdate DATE,
    sportid INT,
    gametype CHAR(1),
    codedgamestate CHAR(1),
    detailedstate TEXT,
    awayteamid INT,
    awayteamname TEXT,
    awayteamscore SMALLINT,
    hometeamid INT,
    hometeamname TEXT,
    hometeamscore SMALLINT,
    venueid INT,
    venuename TEXT,
    scheduledinnings SMALLINT
);

CREATE TABLE linescore (
    gamepk INT REFERENCES game(gamepk),
    inning SMALLINT,
    half SMALLINT,
    battingteamid INT,
    runs SMALLINT,
    hits SMALLINT,
    errors SMALLINT,
    leftonbase SMALLINT,
    battingteam_score SMALLINT,
    battingteam_score_diff SMALLINT,
    PRIMARY KEY (gamepk, inning, half)
);

CREATE TABLE runner_play (
    gamepk INT REFERENCES game(gamepk),
    atbatindex SMALLINT,
    playindex SMALLINT,
    runnerid INT,
    playid UUID,
    runnerfullname TEXT,
    startbase VARCHAR(2),
    endbase VARCHAR(2),
    reachedbase VARCHAR(2),
    is_out BOOLEAN,
    eventtype TEXT,
    movementreason TEXT,
    is_risp BOOLEAN,
    is_firsttothird BOOLEAN,
    is_secondtohome BOOLEAN,
    PRIMARY KEY (gamepk, atbatindex, playindex, runnerid)
);