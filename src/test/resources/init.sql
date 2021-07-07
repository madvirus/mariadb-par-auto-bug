create table test.month_visitor (
  id BIGINT(20) auto_increment,
  ym INT not null,
  loginid varchar(200) not null,
  reg datetime default current_timestamp(),
  primary key(id, ym),
  unique index (ym, loginid)
)
COLLATE='utf8_general_ci'
partition by range(ym)
(
    partition `p2020` values less than (202101) engine = InnoDB,
    partition `p2021` values less than (202201) engine = InnoDB,
    partition `p2022` values less than (202301) engine = InnoDB
)
;
