select id, id_type, count(map(neighbour_id, neighbour_id_type)) as neighbours_number
from {edges_table}
where id_type = '{id_type}'
group by id, id_type
