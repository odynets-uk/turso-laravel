<?php

declare(strict_types=1);

namespace RichanFongdasen\Turso\Database;

use Exception;
use Illuminate\Database\Connection;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Str;
use PDO;
use RichanFongdasen\Turso\Jobs\TursoSyncJob;

class TursoConnection extends Connection
{
    /**
     * Track if any write operations were executed
     */
    protected bool $hasModifiedRecords = false;
        
    public function __construct(TursoPDO $pdo, string $database = ':memory:', string $tablePrefix = '', array $config = [])
    {
         // ВАЖЛИВО: Перевіряємо і 'db_url', і 'url', щоб підтримати різні варіанти конфігурації
        foreach (['url', 'db_url'] as $key) {
            if (isset($config[$key]) && is_string($config[$key]) && Str::startsWith($config[$key], 'libsql:')) {
                // Замінюємо libsql: на https: для коректної роботи cURL/Guzzle
                $config[$key] = Str::replaceFirst('libsql:', 'https:', $config[$key]);
            }
        }

        parent::__construct($pdo, $database, $tablePrefix, $config);

        $this->schemaGrammar = $this->getDefaultSchemaGrammar();
    }

    /**
     * Run an insert statement against the database.
     */
    public function insert($query, $bindings = []): bool
    {
        $result = parent::insert($query, $bindings);
        
        if ($result) {
            $this->hasModifiedRecords = true;
        }
        
        return $result;
    }

    /**
     * Run an update statement against the database.
     */
    public function update($query, $bindings = []): int
    {
        $result = parent::update($query, $bindings);
        
        if ($result > 0) {
            $this->hasModifiedRecords = true;
        }
        
        return $result;
    }

    public function createReadPdo(array $config = []): ?PDO
    {
        $replicaPath = (string) data_get($config, 'db_replica');

        if (($replicaPath === '') || ! file_exists($replicaPath)) {
            return null;
        }

        $pdo = new PDO('sqlite:' . $replicaPath);

        $this->setReadPdo($pdo);

        return $pdo;
    }

    /**
     * Run a delete statement against the database.
     */
    public function delete($query, $bindings = []): int
    {
        $result = parent::delete($query, $bindings);
        
        if ($result > 0) {
            $this->hasModifiedRecords = true;
        }
        
        return $result;
    }

    /**
     * Execute an SQL statement and return the boolean result.
     */
    public function statement($query, $bindings = []): bool
    {
        $result = parent::statement($query, $bindings);
        
        // Check if it's a write operation
        $sql = strtoupper(trim($query));
        if (
            str_starts_with($sql, 'INSERT') ||
            str_starts_with($sql, 'UPDATE') ||
            str_starts_with($sql, 'DELETE') ||
            str_starts_with($sql, 'REPLACE')
        ) {
            $this->hasModifiedRecords = true;
        }
        
        return $result;
    }

    /**
     * Check if any write operations were executed on this connection.
     */
    public function hasModifiedRecords(): bool
    {
        return $this->hasModifiedRecords;
    }

    /**
     * Reset the modified records flag.
     */
    public function resetModifiedRecords(): void
    {
        $this->hasModifiedRecords = false;
    }
    
    protected function escapeBinary(mixed $value): string
    {
        $hex = bin2hex($value);

        return "x'{$hex}'";
    }

    protected function getDefaultPostProcessor(): TursoQueryProcessor
    {
        return new TursoQueryProcessor();
    }

    protected function getDefaultQueryGrammar(): TursoQueryGrammar
    {
        // 1. Передаємо $this у конструктор (виправляє ArgumentCountError)
        $grammar = new TursoQueryGrammar($this);
        
        // 2. Встановлюємо префікс вручну (виправляє BadMethodCallException)
        // Замість $this->withTablePrefix($grammar);
        $grammar->setTablePrefix($this->tablePrefix);

        return $grammar;
    }

    protected function getDefaultSchemaGrammar(): TursoSchemaGrammar
    {
        // 1. Передаємо $this у конструктор
        $grammar = new TursoSchemaGrammar($this);

        // 2. Встановлюємо префікс вручну
        // Замість $this->withTablePrefix($grammar);
        $grammar->setTablePrefix($this->tablePrefix);

        return $grammar;
    }

    public function getSchemaBuilder(): TursoSchemaBuilder
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new TursoSchemaBuilder($this);
    }

    public function getSchemaState(?Filesystem $files = null, ?callable $processFactory = null): TursoSchemaState
    {
        return new TursoSchemaState($this, $files, $processFactory);
    }

    protected function isUniqueConstraintError(Exception $exception): bool
    {
        return boolval(preg_match('#(column(s)? .* (is|are) not unique|UNIQUE constraint failed: .*)#i', $exception->getMessage()));
    }

    public function sync(): void
    {
        Artisan::call('turso:sync', ['connectionName' => $this->getName()]);
        $this->resetModifiedRecords();
    }

    public function backgroundSync(): void
    {
        TursoSyncJob::dispatch((string) $this->getName());
        $this->enableQueryLog();
        $this->resetModifiedRecords();
    }

    public function disableQueryLog(): void
    {
        parent::disableQueryLog();

        $this->tursoPdo()->getClient()->disableQueryLog();
    }

    public function enableQueryLog(): void
    {
        parent::enableQueryLog();

        $this->tursoPdo()->getClient()->enableQueryLog();
    }

    public function flushQueryLog(): void
    {
        parent::flushQueryLog();

        $this->tursoPdo()->getClient()->flushQueryLog();
    }

    public function getQueryLog()
    {
        return $this->tursoPdo()->getClient()->getQueryLog()->toArray();
    }

    public function tursoPdo(): TursoPDO
    {
        if (! $this->pdo instanceof TursoPDO) {
            throw new Exception('The current PDO instance is not an instance of TursoPDO.');
        }

        return $this->pdo;
    }
}
