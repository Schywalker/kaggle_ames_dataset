          #database class
import pymysql as MySQLdb

import sys

class DbHelper:

    global TBL_AMES_TRAIN
    global SQL_CREATE_TABLE

    global host
    global passwd
    global user
    global dbname
    global port

    #host = "localhost"
    #user = "root"
    host = "west2-mysql-instance1.ctrzwyr54voz.us-west-2.rds.amazonaws.com"
    port = 3306
    user = "awsuser"
    passwd = "nellierova"
    dbname = "db_ames"

    TBL_AMES_TRAIN = "ames_train"

    
    Id= "Id"
    COLUMN_PRIMARY_KEY="PRIMARY_KEY"
    MSSubClass= "MSSubClass"
    MSZoning= "MSZoning"
    LotFrontage= "LotFrontage"
    LotArea= "LotArea"
    Street= "Street"
    Alley= "Alley"
    LotShape= "LotShape"
    LandContour= "LandContour"
    Utilities= "Utilities"
    LotConfig= "LotConfig"
    LandSlope= "LandSlope"
    Neighborhood= "Neighborhood"
    Condition1= "Condition1"
    Condition2= "Condition2"
    BldgType= "BldgType"
    HouseStyle= "HouseStyle"
    OverallQual= "OverallQual"
    OverallCond= "OverallCond"
    YearBuilt= "YearBuilt"
    YearRemodAdd= "YearRemodAdd"
    RoofStyle= "RoofStyle"
    RoofMatl= "RoofMatl"
    Exterior1st= "Exterior1st"
    Exterior2nd= "Exterior2nd"
    MasVnrType= "MasVnrType"
    MasVnrArea= "MasVnrArea"
    ExterQual= "ExterQual"
    ExterCond= "ExterCond"
    Foundation= "Foundation"
    BsmtQual= "BsmtQual"
    BsmtCond= "BsmtCond"
    BsmtExposure= "BsmtExposure"
    BsmtFinType1= "BsmtFinType1"
    BsmtFinSF1= "BsmtFinSF1"
    BsmtFinType2= "BsmtFinType2"
    BsmtFinSF2= "BsmtFinSF2"
    BsmtUnfSF= "BsmtUnfSF"
    TotalBsmtSF= "TotalBsmtSF"
    Heating= "Heating"
    HeatingQC= "HeatingQC"
    CentralAir= "CentralAir"
    Electrical= "Electrical"
    FirstFlrSF= "1stFlrSF"
    SecndFlrSF= "2ndFlrSF"
    LowQualFinSF= "LowQualFinSF"
    GrLivArea= "GrLivArea"
    BsmtFullBath= "BsmtFullBath"
    BsmtHalfBath= "BsmtHalfBath"
    FullBath= "FullBath"
    HalfBath= "HalfBath"
    BedroomAbvGr= "BedroomAbvGr"
    KitchenAbvGr= "KitchenAbvGr"
    KitchenQual= "KitchenQual"
    TotRmsAbvGrd= "TotRmsAbvGrd"
    Functional= "Functional"
    Fireplaces= "Fireplaces"
    FireplaceQu= "FireplaceQu"
    GarageType= "GarageType"
    GarageYrBlt= "GarageYrBlt"
    GarageFinish= "GarageFinish"
    GarageCars= "GarageCars"
    GarageArea= "GarageArea"
    GarageQual= "GarageQual"
    GarageCond= "GarageCond"
    PavedDrive= "PavedDrive"
    WoodDeckSF= "WoodDeckSF"
    OpenPorchSF= "OpenPorchSF"
    EnclosedPorch= "EnclosedPorch"
    ThreeSsnPorch= "3SsnPorch"
    ScreenPorch= "ScreenPorch"
    PoolArea= "PoolArea"
    PoolQC= "PoolQC"
    Fence= "Fence"
    MiscFeature= "MiscFeature"
    MiscVal= "MiscVal"
    MoSold= "MoSold"
    YrSold= "YrSold"
    SaleType= "SaleType"
    SaleCondition= "SaleCondition"
    SalePrice= "SalePrice"
    

    SQL_CREATE_TABLE = ("create table "
            + TBL_AMES_TRAIN + " ("
            + COLUMN_PRIMARY_KEY + " INTEGER PRIMARY KEY AUTO_INCREMENT,"
            +Id+" integer,"
            +MSSubClass+" integer,"
            +MSZoning+" varchar(22),"
            +LotFrontage+" integer,"
            +LotArea+" integer,"
            +Street+" varchar(22),"
            +Alley+" varchar(22),"
            +LotShape+" varchar(22),"
            +LandContour+" varchar(22),"
            +Utilities+" varchar(22),"
            +LotConfig+" varchar(22),"
            +LandSlope+" varchar(22),"
            +Neighborhood+" varchar(22),"
            +Condition1+" varchar(22),"
            +Condition2+" varchar(22),"
            +BldgType+" varchar(22),"
            +HouseStyle+" varchar(22),"
            +OverallQual+" integer,"
            +OverallCond+" integer,"
            +YearBuilt+" integer,"
            +YearRemodAdd+" integer,"
            +RoofStyle+" varchar(22),"
            +RoofMatl+" varchar(22),"
            +Exterior1st+" varchar(22),"
            +Exterior2nd+" varchar(22),"
            +MasVnrType+" varchar(22),"
            +MasVnrArea+" integer,"
            +ExterQual+" varchar(22),"
            +ExterCond+" varchar(22),"
            +Foundation+" varchar(22),"
            +BsmtQual+" varchar(22),"
            +BsmtCond+" varchar(22),"
            +BsmtExposure+" varchar(22),"
            +BsmtFinType1+" varchar(22),"
            +BsmtFinSF1+" integer,"
            +BsmtFinType2+" varchar(22),"
            +BsmtFinSF2+" integer,"
            +BsmtUnfSF+" integer,"
            +TotalBsmtSF+" integer,"
            +Heating+" varchar(22),"
            +HeatingQC+" varchar(22),"
            +CentralAir+" varchar(22),"
            +Electrical+" varchar(22),"
            +FirstFlrSF+" integer,"
            +SecndFlrSF+" integer,"
            +LowQualFinSF+" integer,"
            +GrLivArea+" integer,"
            +BsmtFullBath+" integer,"
            +BsmtHalfBath+" integer,"
            +FullBath+" integer,"
            +HalfBath+" integer,"
            +BedroomAbvGr+" integer,"
            +KitchenAbvGr+" integer,"
            +KitchenQual+" varchar(22),"
            +TotRmsAbvGrd+" integer,"
            +Functional+" varchar(22),"
            + Fireplaces +" integer,"
            +FireplaceQu+" varchar(22),"
            +GarageType+" varchar(22),"
            +GarageYrBlt+" integer,"
            +GarageFinish+" varchar(22),"
            +GarageCars+" integer,"
            +GarageArea+" integer,"
            +GarageQual+" varchar(22),"
            +GarageCond+" varchar(22),"
            +PavedDrive+" varchar(22),"
            +WoodDeckSF+" integer,"
            +OpenPorchSF+" integer,"
            +EnclosedPorch+" integer,"
            +ThreeSsnPorch+" integer,"
            +ScreenPorch+" integer,"
            +PoolArea+" integer,"
            +PoolQC+" varchar(22),"
            +Fence+" varchar(22),"
            +MiscFeature+" varchar(22),"
            +MiscVal+" integer,"
            +MoSold+" integer,"
            +YrSold+" integer,"
            +SaleType+" varchar(22),"
            +SaleCondition+" varchar(22),"
            +SalePrice+" integer"
                      + ")" )


    def __init__(self):
    
        # db1 = MySQLdb.connect(host = host, user = user, passwd = passwd)
        # cursor = db1.cursor()
        # sql_createdb = 'CREATE DATABASE ' + dbname
        # cursor.execute(sql_createdb)

        self._host = host,
        self._user = user,
        self._passwd = passwd,
        self._name = dbname,
        self._charset='utf8mb4'
        status = self.connect_db() ##else it gives error about Latin1 cannot encode character. ordinal not in range

        self._cursor.execute('SET NAMES utf8mb4')
        self._cursor.execute("SET CHARACTER SET utf8mb4")
        self._cursor.execute("SET character_set_connection=utf8mb4")

        self._TBL_AMES_TRAIN = TBL_AMES_TRAIN

        # Create Table
        # self.create(AMES_TRAIN, SQL_CREATE_TABLE)

        # self.convert_columns_to_utf8()

    def connect_db(self):
        try:
            self._db = MySQLdb.connect(host = host, user = user, 
                                 passwd = passwd, port = port, db = dbname, charset = 'utf8mb4')

            self._cursor = self._db.cursor()        
            self._cursor.execute("SELECT VERSION()")
            results = self._cursor.fetchone()
            # Check if anything at all is returned
            if results:
                return True
            else:
                return False
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
                # Print results in comma delimited format
                print "ERROR IN CONNECTION" + str(MySQLdb.Error)
        return False
        

    def convert_columns_to_utf8(self):
        ##to convert the table columns to utf-8, since they can still be different
        self._cursor.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'" % dbname)
        
        sql = "SELECT DISTINCT(table_name) FROM information_schema.columns WHERE table_schema = '%s'" % dbname
        self._cursor.execute(sql)
        
        results = self._cursor.fetchall()
        for row in results:
          sql = "ALTER TABLE `%s` convert to character set DEFAULT COLLATE DEFAULT" % (row[0])
          self._cursor.execute(sql)


    def create(self, table, query):
        sql_tb_exists = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + table + "'"
        ##replace above query statement with the one below that allows for wildcard and format correction
        #sql_tb_exists("""
         # SELECT COUNT(*)
          #FROM information_schema.tables
          #WHERE table_name = '{0}'
          #""".format(tablename.replace('\'', '\'\'')))
        c = self._db.cursor()
        c.execute(sql_tb_exists)
        if c.fetchone()[0] != 1:
          c.execute(query)
          self._db.commit()


    def get_connection(self):
        return self._db

    def execute(self, query, params):
        c = self._db.cursor()
        status = c.execute(query, params)
        self._db.commit()
        return status

    def executemany(self, query, params):
        c = self._db.cursor()
        status = c.executemany(query, params)
        self._db.commit()
        return status


    def cursor(self):
        self._db.cursor()


    def commit(self):
        self._db.commit()


    def close(self):
      if self._db is not None:
        self._db.close()


        
    def main():
        main


if __name__ == "__main__":
    main()

    