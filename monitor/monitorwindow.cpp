#include "monitorwindow.h"

#include <QGroupBox>
#include <QHBoxLayout>
#include <QJsonDocument>
#include <QJsonObject>
#include <QVBoxLayout>

// using namespace QtCharts;

MonitorWindow::MonitorWindow(QWidget* parent) : QMainWindow(parent) {
  setupUi();

  ConnectionConfig config;
  config.clientId = "monitor";

  GrpcConnectionManager::init(config);

  GrpcConnectionManager::registerCallback("__SYS_STATS__", [this](const std::string& data) {
    QJsonDocument doc = QJsonDocument::fromJson(QString::fromStdString(data).toUtf8());
    if (doc.isObject()) {
      // Marshall to Main Thread for UI updates
      QMetaObject::invokeMethod(this, [this, doc]() { updateStats(doc.object()); });
    }
  });
}

void MonitorWindow::updateStats(const QJsonObject& json) {
  m_lblClients->setText(QString::number(json["clients"].toInt()));
  m_lblPeers->setText(QString::number(json["peers_count"].toInt()));
  m_lblTotal->setText(QString::number(json["total_msgs"].toVariant().toLongLong()));

  double mps = json["msgs_per_sec"].toDouble();
  double kbps = json["kb_per_sec"].toDouble();

  m_timeCounter++;
  // m_mpsSeries->append(m_timeCounter, mps);
  // m_kbpsSeries->append(m_timeCounter, kbps);

  // Auto-scroll chart (keep last 60 seconds)
  // if (m_timeCounter > 60) {
  //   m_chart->axes(Qt::Horizontal).first()->setMin(m_timeCounter - 60);
  //   m_chart->axes(Qt::Horizontal).first()->setMax(m_timeCounter);
  // }
}

void MonitorWindow::setupUi() {
  QWidget* central = new QWidget(this);
  setCentralWidget(central);
  QVBoxLayout* mainLayout = new QVBoxLayout(central);

  // --- Top Stats Row ---
  QHBoxLayout* topRow = new QHBoxLayout();

  auto createCard = [](QString title, QLabel** lblOut) {
    QGroupBox* gb = new QGroupBox(title);
    QVBoxLayout* l = new QVBoxLayout(gb);
    QLabel* val = new QLabel("0");
    val->setStyleSheet("font-size: 24px; font-weight: bold; color: #00BCD4;");
    val->setAlignment(Qt::AlignCenter);
    l->addWidget(val);
    *lblOut = val;
    return gb;
  };

  topRow->addWidget(createCard("Active Clients", &m_lblClients));
  topRow->addWidget(createCard("Mesh Peers", &m_lblPeers));
  topRow->addWidget(createCard("Total Messages", &m_lblTotal));

  mainLayout->addLayout(topRow);

  // --- Chart Area ---
  // m_chart = new QChart();
  // m_chart->setTitle("Throughput (Live)");
  // m_chart->setAnimationOptions(QChart::NoAnimation);  // Better performance for realtime

  // m_mpsSeries = new QLineSeries();
  // m_mpsSeries->setName("Messages / Sec");
  // m_chart->addSeries(m_mpsSeries);

  // m_kbpsSeries = new QLineSeries();
  // m_kbpsSeries->setName("KB / Sec");
  // m_chart->addSeries(m_kbpsSeries);

  // m_chart->createDefaultAxes();
  // m_chart->axes(Qt::Horizontal).first()->setTitleText("Time (s)");
  // m_chart->axes(Qt::Horizontal).first()->setRange(0, 60);

  // // Make Y axis dynamic? For now 0-1000
  // m_chart->axes(Qt::Vertical).first()->setRange(0, 100);

  // QChartView* chartView = new QChartView(m_chart);
  // chartView->setRenderHint(QPainter::Antialiasing);
  // mainLayout->addWidget(chartView);

  resize(800, 600);
  setWindowTitle("Broker Monitor");
}
