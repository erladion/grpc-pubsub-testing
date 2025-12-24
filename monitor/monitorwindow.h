#ifndef MONITOR_WINDOW_H
#define MONITOR_WINDOW_H

#include <QLabel>
#include <QMainWindow>
#include <QProgressBar>
// #include <QtCharts/QChartView>
// #include <QtCharts/QLineSeries>
// #include <QtCharts/QValueAxis>

#include "grpcconnectionmanager.h"

class MonitorWindow : public QMainWindow {
  Q_OBJECT
public:
  MonitorWindow(QWidget* parent = nullptr);

private slots:
  void updateStats(const QJsonObject& json);

private:
  void setupUi();

  // UI Elements
  QLabel* m_lblClients;
  QLabel* m_lblPeers;
  QLabel* m_lblTotal;

  // Charts
  // QtCharts::QLineSeries* m_mpsSeries;
  // QtCharts::QLineSeries* m_kbpsSeries;
  // QtCharts::QChart* m_chart;

  qint64 m_timeCounter = 0;
};

#endif
