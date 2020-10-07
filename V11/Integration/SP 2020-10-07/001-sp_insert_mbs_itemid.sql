create procedure sp_insert_mbs_itemid(IN i_itemid varchar(50), IN i_isbn varchar(50), IN i_dept varchar(50), IN i_course varchar(50), IN i_section varchar(50), IN i_term varchar(50))
insert into pricer.t_links
        set
            pricer.t_links.itemid = i_itemid,
            pricer.t_links.isbn = i_isbn,
            pricer.t_links.dept = i_dept,
            pricer.t_links.course = i_course,
            pricer.t_links.section = i_section,
            pricer.t_links.term = i_term;

