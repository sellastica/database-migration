<?php
namespace DatabaseMigration;

use Dibi\Connection;
use Sellastica\Maintenance\Maintenance;

class DatabaseMigration
{
	/** @var Connection */
	private $connection;
	/** @var string */
	private $databaseVersionTable = '_db_version';
	/** @var bool */
	private $checkMigrationTable = false;
	/** @var bool */
	private $checkMigrationDir = false;
	/** @var string */
	private $migrationDir;
	/** @var bool */
	private $isDebugMode;


	/**
	 * @param Connection $dibiConnection
	 * @param bool $isDebugMode
	 */
	public function __construct(Connection $dibiConnection, $isDebugMode = false)
	{
		$this->connection = $dibiConnection;
		$this->isDebugMode = $isDebugMode;
	}

	/**
	 * @throws \Exception
	 * @return bool
	 */
	public function process(): bool
	{
		if (Maintenance::is()) {
			return false;
		}

		if ($this->checkMigrationDir) {
			if (!is_dir($this->migrationDir)) {
				if (!mkdir($this->migrationDir, 0775, true)) {
					throw new \Exception('Can to create database migration folder. Check folder permissions.');
				}
			}
		}

		$users = $this->getAvailableUsers();
		$userFilesCount = 0;
		foreach ($users as $user) {
			$userFilesCount += count($user);
		}

		if (!empty($users)) {
			// Create table migration if some migration exists
			if ($this->checkMigrationTable) {
				$this->handleMigrationTable();
			}

			// Fetch actual database version for each user
			$databaseData = $this->connection
				->select('user, version')
				->from($this->databaseVersionTable)
				->fetchPairs();

			$filesToBeMigrated = array();
			foreach ($users as $user => $userFilesIndexes) {
				if (empty($userFilesIndexes)) {
					continue;
				}

				$userFileHighestIndex = $userFilesIndexes[sizeof($userFilesIndexes) - 1];
				// If the user is not in database than migrate all
				if (!array_key_exists($user, $databaseData)) {
					$filesToBeMigrated[$user] = $userFilesIndexes;

				} else if ($databaseData[$user] < $userFileHighestIndex) {
					for ($idx = $databaseData[$user] + 1; $idx <= $userFileHighestIndex; $idx++) {
						$filesToBeMigrated[$user][] = $idx;
					}

				} elseif ($userFileHighestIndex < $databaseData[$user]) {
					throw new \Exception(
						sprintf('
							Higher database version (%d) then migration files (%d): ' . $user,
							$databaseData[$user],
							$userFileHighestIndex
						)
					);
				}
			}

			if (!empty($filesToBeMigrated)) {
				$this->migrateFiles($filesToBeMigrated);
			}
		}

		return true;
	}

	/**
	 * @param bool $checkMigrationTable
	 */
	public function setCheckMigrationTable(bool $checkMigrationTable)
	{
		$this->checkMigrationTable = $checkMigrationTable;
	}

	/**
	 * @param boolean $checkMigrationDir
	 */
	public function setCheckMigrationDir($checkMigrationDir)
	{
		$this->checkMigrationDir = $checkMigrationDir;
	}

	/**
	 * @param string $migrationDir
	 */
	public function setMigrationDir(string $migrationDir)
	{
		$this->migrationDir = $migrationDir;
	}

	/**
	 * @param array $filesToBeMigrated
	 * @return bool
	 * @throws \Exception
	 */
	private function migrateFiles(array $filesToBeMigrated)
	{
		if (Maintenance::is()) {
			return false;
		}

		if (!$this->isDebugMode) {
			Maintenance::setLocal();
		}

		foreach ($filesToBeMigrated as $user => $fileIndexes) {
			foreach ($fileIndexes as $idx) {
				$filename = str_pad($idx, 4, 0, STR_PAD_LEFT) . '.sql';
				$filepath = $this->migrationDir . '/' . $user . '/' . $filename;
				if (true === file_exists($filepath)) {
					$this->connection->loadFile($filepath);
					if ($idx == 1) {
						$array = array(
							'user' => $user,
							'version' => $idx,
							'date' => date('Y-m-d H:i:s'),
						);
						$this->connection
							->insert($this->databaseVersionTable, $array)
							->execute();
					} else {
						$array = array(
							'version' => $idx,
							'date' => date('Y-m-d H:i:s')
						);
						$this->connection->update($this->databaseVersionTable, $array)
							->where('user = %s', $user)
							->execute();
					}
				} else {
					throw new \Exception(sprintf('The migration file %s does not exist.', $filepath));
				}
			}
		}

		if (!$this->isDebugMode) {
			Maintenance::removeLocal();
		}

		return true;
	}

	/**
	 * Finds all user folders and also counts files count in every user folder
	 *
	 * @return array
	 */
	private function getAvailableUsers()
	{
		$users = array();
		foreach (new \DirectoryIterator($this->migrationDir) as $userFolder) {
			if (!$userFolder->isDir() || $userFolder->isDot()) {
				continue;
			}

			$files = array();
			foreach (new \DirectoryIterator($userFolder->getPathname()) as $file) {
				if (!$file->isFile()
					|| pathinfo($file->getBasename(), PATHINFO_EXTENSION) !== 'sql'
				) {
					continue;
				}

				$files[] = (int)pathinfo($file->getBasename(), PATHINFO_FILENAME);
			}

			if (!empty($files)) {
				sort($files);
			}

			$users[$userFolder->getBasename()] = $files;
		}

		return $users;
	}

	/**
	 * Creates table used for database migration
	 * @return void
	 */
	private function handleMigrationTable(): void
	{
		$this->connection->query('
			CREATE TABLE IF NOT EXISTS ' . $this->databaseVersionTable . ' (
				user VARCHAR(10) NOT NULL,
				version SMALLINT UNSIGNED NOT NULL,
				date DATETIME NOT NULL,
				PRIMARY KEY (user)
			);
		');
	}
}
