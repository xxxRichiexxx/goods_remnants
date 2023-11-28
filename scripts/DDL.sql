DROP TABLE IF EXISTS sttgaz.stage_isc_goods_remnants;
CREATE TABLE sttgaz.stage_isc_goods_remnants (
    status VARCHAR(300)
    ,VidTovaraPoDivisionu VARCHAR(300)
    ,ZaiavkaVerhnegoUrovnia_NapravlenieRealizacii VARCHAR(500)
    ,PrognozRealizacii VARCHAR(1000)
    ,Zaiavka_Pokupatel  VARCHAR(500)
    ,Rezerv  VARCHAR(500)
    ,TovarCod65  VARCHAR(500)
    ,GruppaCveta  VARCHAR(500)
    ,VIN  VARCHAR(500)
    ,NomernoiTovar  VARCHAR(500)
    ,Zaiavka_MesiacOtgruzki  VARCHAR(300)
    ,VariantSborkiProdazi_Facticheskii  VARCHAR(1000)
    ,PriznakRezervirovaniya  VARCHAR(500)
    ,ModelniyGod_Periodicheskiy  INT
    ,GosudarstvenniyContract_IGK VARCHAR(500)
    ,Defect  VARCHAR(500)
    ,Kolichestvo INT 
    ,StoimostHraneniyaSNDS  VARCHAR(300)
    ,StoimostHraneniyaBezNDS  VARCHAR(500)
    ,NDSOtStoimostiHraneniya VARCHAR(500)
    ,PokupatelIzZaiavkiVerhnegoUrovnia VARCHAR(1000)
    ,Sklad VARCHAR(500)
    ,VidSklada VARCHAR(500)
    ,DataPrihodaNaSkladGotovogoAM VARCHAR(300)
    ,load_date DATE
);


DROP TABLE IF EXISTS sttgaz.dm_isc_goods_remnants;
CREATE TABLE sttgaz.dm_isc_goods_remnants (
    status VARCHAR(300)
    ,VidTovaraPoDivisionu VARCHAR(300)
    ,ZaiavkaVerhnegoUrovnia_NapravlenieRealizacii VARCHAR(500)
    ,PrognozRealizacii VARCHAR(1000)
    ,Zaiavka_Pokupatel  VARCHAR(500)
    ,Rezerv  VARCHAR(500)
    ,TovarCod65  VARCHAR(500)
    ,GruppaCveta  VARCHAR(500)
    ,VIN  VARCHAR(500)
    ,NomernoiTovar  VARCHAR(500)
    ,Zaiavka_MesiacOtgruzki  VARCHAR(300)
    ,VariantSborkiProdazi_Facticheskii  VARCHAR(1000)
    ,PriznakRezervirovaniya  VARCHAR(500)
    ,ModelniyGod_Periodicheskiy  INT
    ,GosudarstvenniyContract_IGK VARCHAR(500)
    ,Defect  VARCHAR(500)
    ,Kolichestvo INT 
    ,StoimostHraneniyaSNDS  VARCHAR(300)
    ,StoimostHraneniyaBezNDS  VARCHAR(500)
    ,NDSOtStoimostiHraneniya VARCHAR(500)
    ,PokupatelIzZaiavkiVerhnegoUrovnia VARCHAR(1000)
    ,Sklad VARCHAR(500)
    ,VidSklada VARCHAR(500)
    ,DataPrihodaNaSkladGotovogoAM VARCHAR(300)
    ,load_date DATE
);


