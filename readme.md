Example tested commands (sql and node specific). Please note that we need a where clause even to return all rows, therefore a always true statetment (like where 1 = 1) is needed for now:

.\np.exe query "insert into tabcliente (clientid, name, address, country , email) VALUES (3, 'Julio', 'Fazenda', 'BR', 'julio@someplace.com')" <br>
.\np.exe query "select * from tabcliente where name = 'Elmo'" <br>
.\np.exe query "select client_number as name from table1 where name_client = 'teste3'" <br>
.\np.exe query "select name_client, client_number from (select client_number, name_client from (select client_number, name_client from table1 where 1 = 1 ) as tab2 where client_number = 3 ) as tab where name_client = 'teste3' " <br>
.\np.exe query "select client_number, ( case when client_number = 3 then client_number when client_number > 7 then name_client else 'teste' end ) as campo  from table1 where 1 = 1" <br>
.\np.exe query "DELETE FROM tabcliente where email = 'julio@someplace.com'" <br>
.\np.exe addnode -port 10004 -name voodoo -hostname localhost <br>
