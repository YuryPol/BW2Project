-- this try works.
call GetItemsFromSD(1,400000);
call GetItemsFromSD(2,600000);
call GetItemsFromSD(4,10000);
call GetItemsFromSD(8,280000);
call GetItemsFromSD(16,10000);
call GetItemsFromSD(64,10000);

-- another try with error
call GetItemsFromSD(2,10000);
call GetItemsFromSD(4,80000);
call GetItemsFromSD(64,50000);
call GetItemsFromSD(8,200000);
call GetItemsFromSD(2,50000);
call GetItemsFromSD(1,200000);
call GetItemsFromSD(16,50000);
call GetItemsFromSD(2,60000);
call GetItemsFromSD(1,150000);
call GetItemsFromSD(16,50000);
call GetItemsFromSD(64,30000);
call GetItemsFromSD(2,300000);
call GetItemsFromSD(16,80000);
