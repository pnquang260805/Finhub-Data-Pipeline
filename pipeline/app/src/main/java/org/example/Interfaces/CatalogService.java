package org.example.Interfaces;

public interface CatalogService {
    void createCatalog(String catalogName, String warehouseDir);
    void createDatabase(String catalog, String dbName);
    String createFullPath(String catalog, String db, String table);
}
