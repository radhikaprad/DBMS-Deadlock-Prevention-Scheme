# DBMS Deadlock Prevention Scheme
  When Transaction Ti requests a data item currently held by Tj, Ti is allowed to wait only if it has a timestamp larger than that of Tj, otherwise Tj is rolled back (Tj is wounded by Ti)
  This scheme, allows the younger transaction to wait; but when an older transaction requests an item held by a younger one, the older transaction forces the younger one to abort and release the item.
