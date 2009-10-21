using System;
using System.Windows;

namespace DbCopy
{

	enum AppThemes {
		Aero,
		Luna,
		LunaMettalic,
		LunaHomestead,
		Royale
	}

	/// <summary>
	/// Interaction logic for App.xaml
	/// </summary>
	public partial class App : Application
	{

		private void Application_Startup(object sender, StartupEventArgs e)
		{
			//SetTheme(AppThemes.Aero);
		}

		private void Application_Exit(object sender, ExitEventArgs e)
		{
		}

		private void SetTheme(AppThemes theme)
		{
			Uri uri;
			switch (theme) {
				case AppThemes.Aero:
					uri = new Uri("PresentationFramework.Aero;V3.0.0.0;31bf3856ad364e35;component\\themes/Aero.NormalColor.xaml", UriKind.Relative);
					break;
				case AppThemes.Luna:
					uri = new Uri("PresentationFramework.Luna;V3.0.0.0;31bf3856ad364e35;component\\themes/Luna.NormalColor.xaml", UriKind.Relative);
					break;
				case AppThemes.LunaHomestead:
					uri = new Uri("PresentationFramework.Luna;V3.0.0.0;31bf3856ad364e35;component\\themes/Luna.Homestead.xaml", UriKind.Relative);
					break;
				case AppThemes.LunaMettalic:
					uri = new Uri("PresentationFramework.Luna;V3.0.0.0;31bf3856ad364e35;component\\themes/Luna.Metallic.xaml", UriKind.Relative);
					break;
				case AppThemes.Royale:
					uri = new Uri("PresentationFramework.Royale;V3.0.0.0;31bf3856ad364e35;component\\themes/Royale.NormalColor.xaml", UriKind.Relative);
					break;
				default:
					return;
			}
			Resources.MergedDictionaries.Add(LoadComponent(uri) as ResourceDictionary);
		}

	}
}
