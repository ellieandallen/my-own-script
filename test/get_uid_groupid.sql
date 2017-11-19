select b.uid, b.value, a.id, a.groupid from db.t_user_authentication b
left join midatadb.r_company_mapping a
on b.value=a.id
where b.entry_id=143 and length(b.value)>0
and b.uid in (select distinct uid from db.t_bill)
into outfile './uid_groupid.csv' fields terminated by '\t';
