create 'Students','Stulnfo','Grades'
create 'Courses','C_Info'

put 'Courses','123001','C_Info:C_Name','Math'
put 'Courses','123001','C_Info:C_Credit','2.0'
put 'Courses','123002','C_Info:C_Name','Computer Science'
put 'Courses','123002','C_Info:C_Credit','5.0'
put 'Courses','123003','C_Info:C_Name','English'
put 'Courses','123003','C_Info:C_Credit','3.0'
put 'Students', '2015001', 'Stulnfo:S_Name', 'Li Lei'
put 'Students', '2015001', 'Stulnfo:S_Sex', 'male'
put 'Students', '2015001', 'Stulnfo:S_Age', '23'
put 'Students', '2015001', 'Grades:123001', '86'
put 'Students', '2015001', 'Grades:123003', '69'
put 'Students', '2015002', 'Stulnfo:S_Name', 'Han Meimei'
put 'Students', '2015002', 'Stulnfo:S_Sex', 'female'
put 'Students', '2015002', 'Stulnfo:S_Age', '22'
put 'Students', '2015002', 'Grades:123002', '77'
put 'Students', '2015002', 'Grades:123003', '99'
put 'Students', '2015003', 'Stulnfo:S_Name', 'Zhang San'
put 'Students', '2015003', 'Stulnfo:S_Sex', 'male'
put 'Students', '2015003', 'Stulnfo:S_Age', '24'
put 'Students', '2015003', 'Grades:123001', '98'
put 'Students', '2015003', 'Grades:123002', '95'
scan 'Students'
scan 'Courses'

scan 'Courses', FILTER => "SingleColumnValueFilter('C_Info', 'C_Name', =, 'binary:Computer Science')"
scan 'Students', FILTER => "QualifierFilter(=,'binary:123002')"

alter 'Students','Contact'
put 'Students','2015001','Contact:Email','lilie@qq.com'
put 'Students','2015002','Contact:Email','hmm@qq.com'
put 'Students','2015003','Contact:Email','zs@qq.com'

get 'Students', '2015003', FILTER => " FamilyFilter(=, 'binary:Grades')"
delete 'Students', '2015003', 'Grades:123001'
delete 'Students', '2015003', 'Grades:123002'
scan 'Students'

disable 'Students'
drop 'Students'
disable 'Courses'
drop 'Courses'
list