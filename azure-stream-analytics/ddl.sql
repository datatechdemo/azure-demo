

Create table fraudcall(
  CallrecTime	VARCHAR(264)
, SwitchNum	VARCHAR(264)
, CallingNum	VARCHAR(264)
, CallingIMSI	VARCHAR(264)
, CalledNum	VARCHAR(264)
, CalledIMSI	VARCHAR(264)
) WITH (DISTRIBUTION = ROUND_ROBIN)

